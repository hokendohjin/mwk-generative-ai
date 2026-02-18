import { useState, useCallback } from 'react';
import useChatApi from './useChatApi';
import { MODELS } from './useModel';
import { getPrompter } from '../prompts';
import {
  UnrecordedMessage,
  Model,
  Metadata,
  ToBeRecordedMessage,
} from 'generative-ai-use-cases';
import { v4 as uuidv4 } from 'uuid';

export type MeetingMinutesStyle =
  | 'faq'
  | 'newspaper'
  | 'transcription'
  | 'custom';

export const useMeetingMinutes = (
  minutesStyle: MeetingMinutesStyle,
  customPrompt: string,
  autoGenerateSessionTimestamp: number | null,
  setGeneratedMinutes: (minutes: string) => void,
  setLastProcessedTranscript: (transcript: string) => void,
  setLastGeneratedTime: (time: Date | null) => void
) => {
  const { predictStream, createChat, createMessages, predictTitle } =
    useChatApi();
  const { modelIds: availableModels, textModels } = MODELS;

  // Only keep local state for temporary values
  const [loading, setLoading] = useState(false);

  const generateMinutes = useCallback(
    async (
      transcript: string,
      modelId: string,
      onGenerate?: (
        status: 'generating' | 'success' | 'error',
        data?: { message?: string; minutes?: string }
      ) => void
    ) => {
      if (!transcript || transcript.trim() === '') return;

      const model = textModels.find((m) => m.modelId === modelId);
      if (!model) {
        onGenerate?.('error', { message: 'Model not found' });
        return;
      }

      setLoading(true);
      onGenerate?.('generating');

      try {
        const prompter = getPrompter(modelId);

        const promptContent =
          minutesStyle === 'custom' && customPrompt
            ? customPrompt
            : prompter.meetingMinutesPrompt({
                style: minutesStyle,
                customPrompt,
              });

        const messages: UnrecordedMessage[] = [
          {
            role: 'system',
            content: promptContent,
          },
          {
            role: 'user',
            content: transcript,
          },
        ];

        const stream = predictStream({
          model: model as Model,
          messages,
          id: `meeting-minutes-${autoGenerateSessionTimestamp || Date.now()}`,
        });

        let fullResponse = '';
        let lastMetadata: Metadata | undefined;
        setGeneratedMinutes('');

        for await (const chunk of stream) {
          if (chunk) {
            const chunks = (chunk as string).split('\n');

            for (const c of chunks) {
              if (c && c.length > 0) {
                try {
                  const payload = JSON.parse(c) as {
                    text: string;
                    metadata?: Metadata;
                  };
                  if (payload.text && payload.text.length > 0) {
                    fullResponse += payload.text;
                    setGeneratedMinutes(fullResponse);
                  }
                  if (payload.metadata) {
                    lastMetadata = payload.metadata;
                  }
                } catch (error) {
                  // Skip invalid JSON chunks
                  console.debug('Skipping invalid JSON chunk:', c);
                }
              }
            }
          }
        }

        // Save messages and record token usage
        try {
          const { chat } = await createChat();
          const toBeRecordedMessages: ToBeRecordedMessage[] = [
            {
              role: 'user',
              content: transcript,
              messageId: uuidv4(),
              usecase: '/meeting-minutes',
              llmType: modelId,
            },
            {
              role: 'assistant',
              content: fullResponse,
              messageId: uuidv4(),
              usecase: '/meeting-minutes',
              llmType: modelId,
              metadata: lastMetadata,
            },
          ];
          await createMessages(chat.chatId, { messages: toBeRecordedMessages });

          // Generate title (fire-and-forget)
          predictTitle({
            model: model as Model,
            chat,
            prompt: prompter.setTitlePrompt({
              messages: [
                { role: 'user', content: transcript },
                { role: 'assistant', content: fullResponse },
              ],
            }),
            id: '/title',
          }).catch(() => {});
        } catch (err) {
          console.error('Failed to save messages:', err);
        }

        setLastProcessedTranscript(transcript);
        setLastGeneratedTime(new Date());
        onGenerate?.('success', { minutes: fullResponse });
      } catch (error) {
        onGenerate?.('error', {
          message: error instanceof Error ? error.message : 'Unknown error',
        });
      } finally {
        setLoading(false);
      }
    },
    [
      minutesStyle,
      customPrompt,
      predictStream,
      createChat,
      createMessages,
      predictTitle,
      textModels,
      autoGenerateSessionTimestamp,
      setGeneratedMinutes,
      setLastGeneratedTime,
      setLastProcessedTranscript,
    ]
  );

  const clearMinutes = useCallback(() => {
    setGeneratedMinutes('');
    setLastProcessedTranscript('');
    setLastGeneratedTime(null);
  }, [setGeneratedMinutes, setLastProcessedTranscript, setLastGeneratedTime]);

  return {
    // State
    loading,

    // Actions
    generateMinutes,
    clearMinutes,

    // Utilities
    availableModels,
  };
};

export default useMeetingMinutes;

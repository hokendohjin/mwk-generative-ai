import {
  StreamingChunk,
  Metadata,
  ToBeRecordedMessage,
} from 'generative-ai-use-cases';
import useChatApi from './useChatApi';
import { create } from 'zustand';
import { MODELS } from './useModel';
import { generateWriterPrompt, WriterOption } from '../prompts/writer';
import { v4 as uuidv4 } from 'uuid';

const useWriterState = create<{
  modelId: string;
  setModelId: (modelId: string) => void;
}>((set) => ({
  modelId: MODELS.textModels[0].modelId,
  setModelId: (modelId: string) => set({ modelId }),
}));

export const useWriter = () => {
  const { predictStream, createChat, createMessages } = useChatApi();
  const { modelId, setModelId } = useWriterState();

  const write = async function* (
    prompt: string,
    option: string,
    command?: string
  ): AsyncGenerator<{ text?: string; trace?: string }> {
    const { messages, overrideModel } = generateWriterPrompt(
      prompt,
      option as WriterOption,
      command
    );

    const usedModelId = overrideModel?.modelId || modelId;

    const stream = await predictStream({
      id: '1',
      messages,
      model: overrideModel || {
        type: 'bedrock',
        modelId: modelId,
      },
    });
    let tmpChunk = '';
    let tmpTrace = '';
    let fullResponse = '';
    let lastMetadata: Metadata | undefined;

    for await (const chunk of stream) {
      const chunks = (chunk as string).split('\n');

      for (const c of chunks) {
        if (c && c.length > 0) {
          const payload = JSON.parse(c) as StreamingChunk;

          if (payload.text.length > 0) {
            tmpChunk += payload.text;
            fullResponse += payload.text;
          }

          if (payload.trace) {
            tmpTrace += payload.trace;
          }

          if (payload.metadata) {
            lastMetadata = payload.metadata;
          }
        }

        if (tmpChunk.length >= 10) {
          yield { text: tmpChunk };
          tmpChunk = '';
        }

        if (tmpTrace.length >= 10) {
          yield { trace: tmpTrace };
          tmpTrace = '';
        }
      }

      if (tmpChunk.length > 0) {
        yield { text: tmpChunk };
        tmpChunk = '';
      }

      if (tmpTrace.length > 0) {
        yield { trace: tmpTrace };
        tmpTrace = '';
      }
    }

    // メッセージとトークン使用量を保存
    try {
      const { chatId } = await createChat();
      const toBeRecordedMessages: ToBeRecordedMessage[] = [
        {
          role: 'user',
          content: prompt,
          messageId: uuidv4(),
          usecase: 'writer',
          llmType: usedModelId,
        },
        {
          role: 'assistant',
          content: fullResponse,
          messageId: uuidv4(),
          usecase: 'writer',
          llmType: usedModelId,
          metadata: lastMetadata,
        },
      ];
      await createMessages(chatId, { messages: toBeRecordedMessages });
    } catch (err) {
      console.error('Failed to save messages:', err);
    }
  };

  return {
    write,
    modelId,
    setModelId,
  };
};

export default useWriter;

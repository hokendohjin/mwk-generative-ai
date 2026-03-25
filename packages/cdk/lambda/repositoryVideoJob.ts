import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  UpdateCommand,
  DeleteCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  VideoJob,
  ListVideoJobsResponse,
  GenerateVideoRequest,
} from 'generative-ai-use-cases';
import {
  GetAsyncInvokeCommand,
  ValidationException,
} from '@aws-sdk/client-bedrock-runtime';
import { CopyVideoJobParams } from './copyVideoJob';
import {
  LambdaClient,
  InvokeCommand,
  InvocationType,
} from '@aws-sdk/client-lambda';
import { initBedrockRuntimeClient } from './utils/bedrockClient';

const BUCKET_NAME: string = process.env.BUCKET_NAME!;
const TABLE_NAME: string = process.env.TABLE_NAME!;
const STATS_TABLE_NAME: string = process.env.STATS_TABLE_NAME || '';
const COPY_VIDEO_JOB_FUNCTION_ARN = process.env.COPY_VIDEO_JOB_FUNCTION_ARN!;
const dynamoDb = new DynamoDBClient({});
const dynamoDbDocument = DynamoDBDocumentClient.from(dynamoDb);
const lambda = new LambdaClient({});

export const createJob = async (
  _userId: string,
  invocationArn: string,
  req: GenerateVideoRequest
) => {
  const userId = `videoJob#${_userId}`;
  const jobId = invocationArn.split('/').slice(-1)[0];

  const params = req.params;

  // Do not save the information of the first frame image of Nova Reel in params
  if (params.images && params.images.length > 0) {
    params.images = [];
  }

  const item = {
    id: userId,
    createdDate: `${Date.now()}`,
    jobId,
    invocationArn,
    status: 'InProgress',
    output: `s3://${BUCKET_NAME}/${jobId}/output.mp4`,
    modelId: req.model!.modelId,
    region: req.model!.region,
    ...params,
  };

  await dynamoDbDocument.send(
    new PutCommand({
      TableName: TABLE_NAME,
      Item: item,
    })
  );

  return item;
};

export const updateJobStatus = async (job: VideoJob, status: string) => {
  const updateCommand = new UpdateCommand({
    TableName: TABLE_NAME,
    Key: {
      id: job.id,
      createdDate: job.createdDate,
    },
    UpdateExpression: 'set #status = :status',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
    ExpressionAttributeValues: {
      ':status': status,
    },
  });

  await dynamoDbDocument.send(updateCommand);
};

const checkAndUpdateJob = async (
  job: VideoJob
): Promise<'InProgress' | 'Completed' | 'Failed' | 'Finalizing'> => {
  try {
    const client = await initBedrockRuntimeClient({ region: job.region });
    const command = new GetAsyncInvokeCommand({
      invocationArn: job.invocationArn,
    });

    let res;

    try {
      res = await client.send(command);
    } catch (e) {
      // If it takes time to get the result, GetAsyncInvokeCommand may result in a ValidationException.
      // In such cases, proceed assuming it has reached the Completed state.
      if (e instanceof ValidationException) {
        console.error(e);
        res = { status: 'Completed' as const };
      } else {
        throw e;
      }
    }

    // Video generation is complete, but the video copying is not finished.
    // We will run the copy job to set the status to "Finalizing".
    if (res.status === 'Completed') {
      const params: CopyVideoJobParams = { job };

      await lambda.send(
        new InvokeCommand({
          FunctionName: COPY_VIDEO_JOB_FUNCTION_ARN,
          InvocationType: InvocationType.Event,
          Payload: JSON.stringify(params),
        })
      );

      await updateJobStatus(job, 'Finalizing');
      return 'Finalizing';
    } else if (res.status === 'Failed') {
      // Since video generation has failed, we will not copy the video and will terminate with a Failed status.
      await updateJobStatus(job, 'Failed');
      return 'Failed';
    } else {
      // This res.status will be InProgress only.
      return res.status!;
    }
  } catch (e) {
    console.error(e);
    return job.status;
  }
};

export const listVideoJobs = async (
  _userId: string,
  _exclusiveStartKey?: string
): Promise<ListVideoJobsResponse> => {
  const exclusiveStartKey = _exclusiveStartKey
    ? JSON.parse(Buffer.from(_exclusiveStartKey, 'base64').toString())
    : undefined;

  const userId = `videoJob#${_userId}`;
  const res = await dynamoDbDocument.send(
    new QueryCommand({
      TableName: TABLE_NAME,
      KeyConditionExpression: '#id = :id',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':id': userId,
      },
      ScanIndexForward: false,
      Limit: 10,
      ExclusiveStartKey: exclusiveStartKey,
    })
  );

  const jobs = res.Items as VideoJob[];

  // Check the latest status of InProgress jobs
  for (const job of jobs) {
    if (job.status === 'InProgress') {
      const newStatus = await checkAndUpdateJob(job);
      job.status = newStatus;
    }
  }

  return {
    data: jobs,
    lastEvaluatedKey: res.LastEvaluatedKey
      ? Buffer.from(JSON.stringify(res.LastEvaluatedKey)).toString('base64')
      : undefined,
  };
};

export const deleteVideoJob = async (
  _userId: string,
  createdDate: string
): Promise<void> => {
  const userId = `videoJob#${_userId}`;

  await dynamoDbDocument.send(
    new DeleteCommand({
      TableName: TABLE_NAME,
      Key: {
        id: userId,
        createdDate,
      },
    })
  );
};

export const updateVideoUsage = async (
  userId: string,
  modelId: string
): Promise<void> => {
  if (!STATS_TABLE_NAME) {
    return;
  }

  const dateStr = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  const usecase = '/video';

  try {
    await dynamoDbDocument.send(
      new UpdateCommand({
        TableName: STATS_TABLE_NAME,
        Key: {
          id: `stats#${dateStr}`,
          userId: userId,
        },
        UpdateExpression: `
          SET
            #date = :date,
            executions.#overall = if_not_exists(executions.#overall, :zero) + :one,
            executions.#modelKey = if_not_exists(executions.#modelKey, :zero) + :one,
            executions.#usecaseKey = if_not_exists(executions.#usecaseKey, :zero) + :one,
            inputTokens.#overall = if_not_exists(inputTokens.#overall, :zero) + :zero,
            inputTokens.#modelKey = if_not_exists(inputTokens.#modelKey, :zero) + :zero,
            inputTokens.#usecaseKey = if_not_exists(inputTokens.#usecaseKey, :zero) + :zero,
            outputTokens.#overall = if_not_exists(outputTokens.#overall, :zero) + :zero,
            outputTokens.#modelKey = if_not_exists(outputTokens.#modelKey, :zero) + :zero,
            outputTokens.#usecaseKey = if_not_exists(outputTokens.#usecaseKey, :zero) + :zero,
            cacheReadInputTokens.#overall = if_not_exists(cacheReadInputTokens.#overall, :zero) + :zero,
            cacheReadInputTokens.#modelKey = if_not_exists(cacheReadInputTokens.#modelKey, :zero) + :zero,
            cacheReadInputTokens.#usecaseKey = if_not_exists(cacheReadInputTokens.#usecaseKey, :zero) + :zero,
            cacheWriteInputTokens.#overall = if_not_exists(cacheWriteInputTokens.#overall, :zero) + :zero,
            cacheWriteInputTokens.#modelKey = if_not_exists(cacheWriteInputTokens.#modelKey, :zero) + :zero,
            cacheWriteInputTokens.#usecaseKey = if_not_exists(cacheWriteInputTokens.#usecaseKey, :zero) + :zero,
            audioInputSeconds.#overall = if_not_exists(audioInputSeconds.#overall, :zero) + :zero,
            audioInputSeconds.#modelKey = if_not_exists(audioInputSeconds.#modelKey, :zero) + :zero,
            audioInputSeconds.#usecaseKey = if_not_exists(audioInputSeconds.#usecaseKey, :zero) + :zero,
            audioOutputSeconds.#overall = if_not_exists(audioOutputSeconds.#overall, :zero) + :zero,
            audioOutputSeconds.#modelKey = if_not_exists(audioOutputSeconds.#modelKey, :zero) + :zero,
            audioOutputSeconds.#usecaseKey = if_not_exists(audioOutputSeconds.#usecaseKey, :zero) + :zero
        `,
        ExpressionAttributeNames: {
          '#date': 'date',
          '#overall': 'overall',
          '#modelKey': `model#${modelId}`,
          '#usecaseKey': `usecase#${usecase}`,
        },
        ExpressionAttributeValues: {
          ':date': dateStr,
          ':zero': 0,
          ':one': 1,
        },
      })
    );
  } catch (updateError) {
    console.log(
      'Record does not exist, creating initial structure:',
      updateError
    );
    try {
      const zeroObj = {
        overall: 0,
        [`model#${modelId}`]: 0,
        [`usecase#${usecase}`]: 0,
      };
      await dynamoDbDocument.send(
        new UpdateCommand({
          TableName: STATS_TABLE_NAME,
          Key: {
            id: `stats#${dateStr}`,
            userId: userId,
          },
          UpdateExpression: `
              SET
                #date = :date,
                executions = :executionsObj,
                inputTokens = :zeroObj,
                outputTokens = :zeroObj,
                cacheReadInputTokens = :zeroObj,
                cacheWriteInputTokens = :zeroObj,
                audioInputSeconds = :zeroObj,
                audioOutputSeconds = :zeroObj
          `,
          ExpressionAttributeNames: {
            '#date': 'date',
          },
          ExpressionAttributeValues: {
            ':date': dateStr,
            ':executionsObj': {
              overall: 1,
              [`model#${modelId}`]: 1,
              [`usecase#${usecase}`]: 1,
            },
            ':zeroObj': zeroObj,
          },
        })
      );
    } catch (putError) {
      console.error('Error creating video usage:', putError);
    }
  }
};

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectCannotBeDeletedException;
use Keboola\StorageDriver\Command\Bucket\RevokeBucketAccessFromReadOnlyRoleCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Throwable;

final class RevokeBucketAccessFromReadOnlyRoleHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof RevokeBucketAccessFromReadOnlyRoleCommand);

        assert($runtimeOptions->getMeta() === null);

        assert(
            $command->getBucketObjectName() !== '',
            'RevokeBucketAccessToReadOnlyRoleCommand.bucketObjectName is required',
        );

        $bigQueryClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        // In case of deleting an external bucket, we only need the dataset name.
        // This information is stored in the connection so we just delete the dataset
        $dataset = $bigQueryClient->dataset($command->getBucketObjectName());

        try {
            $dataset->delete();
        } catch (Throwable $e) {
            throw new ObjectCannotBeDeletedException(
                message: DecodeErrorMessage::getDirectErrorMessage($e),
                previous: $e,
            );
        }

        return null;
    }
}

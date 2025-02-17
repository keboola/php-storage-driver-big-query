<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Create;

use Google\ApiCore\ApiException;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDataset;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDatasetReference;
use Google\Cloud\Iam\V1\Binding;
use Google\Protobuf\Internal\Message;
use Google\Rpc\Code;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class GrantBucketAccessToReadOnlyRoleHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof GrantBucketAccessToReadOnlyRoleCommand);

        assert($runtimeOptions->getMeta() === null);

        assert(
            $command->getPath()->count() === 4,
            'GrantBucketAccessToReadOnlyRoleCommand.path is required and size must equal 4',
        );
        assert(
            $command->getDestinationObjectName() !== '',
            'GrantBucketAccessToReadOnlyRoleCommand.getDestinationObjectName is required',
        );

        assert(
            $command->getStackPrefix() !== '',
            'GrantBucketAccessToReadOnlyRoleCommand.getStackPrefix is required',
        );

        $credentialsMeta = CredentialsHelper::getBigQueryCredentialsMeta($credentials);

        [
            $projectId,
            $location,
            $dataExchangerId,
            $listingId,
        ] = $command->getPath();
        $listingName = $this->clientManager->getAnalyticHubClient($credentials)
            ->listingName(
                $projectId,
                $location,
                $dataExchangerId,
                $listingId,
            );

        // This is the name of a bucket created in the target project that represents an external bucket
        $registeredBucketName = $command->getDestinationObjectName();

        $projectCredentials = CredentialsHelper::getCredentialsArray($credentials);

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        // 1. Create new name for dataset
        // In BQ, the linked dataset is physically created in the target project,
        // so we have to generate it from the name we got in the request.
        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            $registeredBucketName,
            $command->getBranchId(),
        );

        $datasetReference = new DestinationDatasetReference();
        $datasetReference->setProjectId($projectCredentials['project_id']);
        $datasetReference->setDatasetId($newBucketDatabaseName);

        $destinationDataset = new DestinationDataset([
            'dataset_reference' => $datasetReference,
            'location' => $credentialsMeta->getRegion(),
        ]);

//        $exchangerName = $this->clientManager->getAnalyticHubClient($credentials)
//            ->getDataExchange(
//                $dataExchangerId,
//                [
//                    'project' => $projectId,
//                    'location' => $location,
//                ],
//            );

        $formattedName = $analyticHubClient->dataExchangeName($projectId, $location, $dataExchangerId);
        $mainCredentials = CredentialsHelper::getCredentialsArray($credentials);
        $iamExchangerPolicy = $analyticHubClient->getIamPolicy($formattedName);
        $binding = $iamExchangerPolicy->getBindings();
        $binding[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $mainCredentials['client_email']],
        ]);

        $iamExchangerPolicy->setBindings($binding);
        $analyticHubClient->setIamPolicy($formattedName, $iamExchangerPolicy);
        try {
            // 2. Then we just need to subscribe to the listing that the user created
            // according to the instruction before he called the bucket registration.
            $analyticHubClient->subscribeListing($listingName, [
                'destinationDataset' => $destinationDataset,
            ]);
        } catch (ApiException $e) {
            if ($e->getCode() === Code::PERMISSION_DENIED) {
                throw SubscribeListingPermissionDeniedException::handlePermissionDeniedException(
                    $e,
                    $registeredBucketName,
                    $listingName,
                );
            }

            if ($e->getCode() === Code::INVALID_ARGUMENT) {
                throw InvalidArgumentException::handleException($e);
            }

            if ($e->getCode() === Code::NOT_FOUND) {
                throw SubscribeListingObjectNotFoundException::handleException($e);
            }

            throw $e;
        }

        // And in response we return the name generated by the generator for the needs of connection
        return (new GrantBucketAccessToReadOnlyRoleResponse())->setCreateBucketObjectName($newBucketDatabaseName);
    }
}

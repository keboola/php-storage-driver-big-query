<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\Iam\V1\Binding;
use Google\Protobuf\Any;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\GrantBucketAccessToReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\InvalidArgumentException;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\SubscribeListingObjectNotFoundException;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop\RevokeBucketAccessFromReadOnlyRoleHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleResponse;
use Keboola\StorageDriver\Command\Bucket\RevokeBucketAccessFromReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Throwable;

class GrantRevokeBucketAccessToReadOnlyRoleTest extends BaseCase
{
    private GenericBackendCredentials $mainProjectCredentials;

    private GenericBackendCredentials $externalProjectCredentials;

    private CreateBucketResponse $bucketResponse;

    protected GenericBackendCredentials $targetProjectCredentials;

    protected CreateProjectResponse $targetProjectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->mainProjectCredentials = $this->projects[0][0];
        $this->externalProjectCredentials = $this->projects[1][0];
        $bucketResponse = $this->createTestBucket($this->projects[1][0], $this->projects[1][2]);
        $this->bucketResponse = $bucketResponse;

        // project2 checks the access
        $this->targetProjectCredentials = $this->projects[1][0];
        $this->targetProjectResponse = $this->projects[1][1];
    }

    public function testRegisterNonExistExchanger(): void
    {
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalTableName = md5($this->getName()) . '_Test_table';
        $this->prepareTestTable($externalBucketName, $externalTableName);
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $externalAnalyticHubClient,
            $externalBucketName,
        );

        $this->grantMainProjectToRegisterExternalBucket($externalAnalyticHubClient, $dataExchange);

        $parsedName = AnalyticsHubServiceClient::parseName($createdListing->getName());

        $handler = new GrantBucketAccessToReadOnlyRoleHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new GrantBucketAccessToReadOnlyRoleCommand())
            ->setPath([
                $parsedName['project'],
                $parsedName['location'],
                $parsedName['data_exchange'],
                'nonexist',
            ])
            ->setDestinationObjectName('test_external')
            ->setBranchId('123')
            ->setStackPrefix($this->getStackPrefix());

        $meta = new Any();
        $meta->pack(
            (new GrantBucketAccessToReadOnlyRoleCommand\GrantBucketAccessToReadOnlyRoleBigqueryMeta()),
        );
        $command->setMeta($meta);

        try {
            $handler(
                $this->mainProjectCredentials,
                $command,
                [],
                new RuntimeOptions(),
            );
            $this->fail('Should fail, because listing does not exist.');
        } catch (SubscribeListingObjectNotFoundException $e) {
            $this->assertSame(
                sprintf(
                    'Failed to find listing: projects/%s/locations/%s/dataExchanges/%s/listings/nonexist',
                    $parsedName['project'],
                    $parsedName['location'],
                    $parsedName['data_exchange'],
                ),
                $e->getMessage(),
            );
        }
    }

    public function testRegisterBucket(): void
    {
        // prepare test external table
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalTableName = md5($this->getName()) . '_Test_table';
        $this->prepareTestTable($externalBucketName, $externalTableName);

        // this part simulate user who want to register ext bucket
        // 1. and 2. will be done in one step, but we need to test it can't be registered before grant permission
        // 1. User which want to register external bucket create exchanged and listing
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $externalAnalyticHubClient,
            $externalBucketName,
        );

        $parsedName = AnalyticsHubServiceClient::parseName($createdListing->getName());

        $handler = new GrantBucketAccessToReadOnlyRoleHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new GrantBucketAccessToReadOnlyRoleCommand())
            ->setPath([
                $parsedName['project'],
                $parsedName['location'],
                $parsedName['data_exchange'],
                $parsedName['listing'],
            ])
            ->setDestinationObjectName('test_external')
            ->setBranchId('123')
            ->setStackPrefix($this->getStackPrefix());
        $meta = new Any();
        $meta->pack(
            (new GrantBucketAccessToReadOnlyRoleCommand\GrantBucketAccessToReadOnlyRoleBigqueryMeta()),
        );
        $command->setMeta($meta);
//        try {
//            $handler(
//                $this->mainProjectCredentials,
//                $command,
//                [],
//                new RuntimeOptions(),
//            );
//            $this->fail('Should not be able to register bucket from another project, until user grant subscription.');
//        } catch (Throwable $e) {
//            $msg = sprintf(
//                'Failed to register external bucket "test_external" permission denied for subscribe listing "%s"',
//                $createdListing->getName(),
//            );
//            $this->assertSame($msg, $e->getMessage());
//        }

        // 2. Grant subscribe permission to external bucket to service account if destination project
        $this->grantMainProjectToRegisterExternalBucket($externalAnalyticHubClient, $dataExchange);

        /** @var GrantBucketAccessToReadOnlyRoleResponse $result */
        $result = $handler(
            $this->mainProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertSame('123_test_external', $result->getCreateBucketObjectName());
        // Validate is bucket added
        $mainBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->mainProjectCredentials);
        $registeredExternalBucketInMainProject = $mainBqClient->dataset('123_test_external');
        $registeredTables = $registeredExternalBucketInMainProject->tables();
        $this->assertCount(1, $registeredTables);

        // And I can get rows from external table
        $result = $mainBqClient->runQuery(
            $mainBqClient->query('SELECT * FROM `123_test_external`.`' . $externalTableName . '`'),
        );
        $this->assertCount(3, $result);

        $this->assertEqualsCanonicalizing(
            [
                [
                    'id' => '1',
                    'name' => 'external',
                    'large' => 'data',
                ],
                [
                    'id' => '2',
                    'name' => 'data from',
                    'large' => 'external table',
                ],
                [
                    'id' => '3',
                    'name' => 'it works',
                    'large' => 'awesome !',
                ],
            ],
            iterator_to_array($result->rows()),
        );

        // Add more data to test rows will be added in main project
        $insertGroups = [
            [
                'columns' => '`id`, `name`, `large`',
                'rows' => [
                    "4, 'more', 'data'",
                ],
            ],
        ];
        $this->fillTableWithData(
            $this->externalProjectCredentials,
            $externalBucketName,
            $externalTableName,
            $insertGroups,
        );

        // And I can get rows from external table
        $result = $mainBqClient->runQuery(
            $mainBqClient->query('SELECT * FROM `123_test_external`.`' . $externalTableName . '`'),
        );
        $this->assertCount(4, $result);

        $this->assertEqualsCanonicalizing(
            [
                [
                    'id' => '1',
                    'name' => 'external',
                    'large' => 'data',
                ],
                [
                    'id' => '2',
                    'name' => 'data from',
                    'large' => 'external table',
                ],
                [
                    'id' => '3',
                    'name' => 'it works',
                    'large' => 'awesome !',
                ],
                [
                    'id' => '4',
                    'name' => 'more',
                    'large' => 'data',
                ],
            ],
            iterator_to_array($result->rows()),
        );

        $handler = new RevokeBucketAccessFromReadOnlyRoleHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new RevokeBucketAccessFromReadOnlyRoleCommand())
            ->setBucketObjectName('123_test_external');
//        $handler(
//            $this->mainProjectCredentials,
//            $command,
//            [],
//            new RuntimeOptions(['runId' => $this->testRunId]),
//        );

        try {
            $mainBqClient->runQuery(
                $mainBqClient->query('SELECT * FROM `123_test_external`.`' . $externalTableName . '`'),
            );
            $this->fail('Should not be able to get data from external table after revoke access.');
        } catch (NotFoundException $e) {
            $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
            $mainProjectStringId = $mainCredentials['project_id'];

            /** @var array<string, array<string, string>> $message */
            $message = json_decode($e->getMessage(), true, 512, JSON_THROW_ON_ERROR);
            assert($message !== null);
            assert(isset($message['error']['message']));
            $this->assertSame(
                sprintf('Not found: Dataset %s:123_test_external was not found in location US', $mainProjectStringId),
                $message['error']['message'],
            );
        }
    }

    public function testRegisterBucketInDifferentRegionShouldFail(): void
    {
        $externalTableName = $this->getTestHash() . '_Test_table';
        $bucketId = $this->getTestHash() . 'bucket_in_eu';
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->externalProjectCredentials);

        // manually create dataset with table in EU
        $dataset = $bqClient->dataset($bucketId);
        try {
            $dataset->delete(['deleteContents' => true]);
        } catch (Throwable) {
            // ignore
        }
        $bucket = $bqClient->createDataset($bucketId, ['location' => 'EU']);
        $bucket->createTable($externalTableName);

        // create exchange and listing for external bucket in EU
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $externalAnalyticHubClient,
            $bucketId,
            'EU',
        );

        $parsedName = AnalyticsHubServiceClient::parseName($createdListing->getName());

        $handler = new GrantBucketAccessToReadOnlyRoleHandler($this->clientManager);
        $command = (new GrantBucketAccessToReadOnlyRoleCommand())
            ->setPath([
                $parsedName['project'],
                $parsedName['location'],
                $parsedName['data_exchange'],
                $parsedName['listing'],
            ])
            ->setDestinationObjectName('test_external')
            ->setBranchId('123')
            ->setStackPrefix($this->getStackPrefix());

        $meta = new Any();
        $meta->pack(
            (new GrantBucketAccessToReadOnlyRoleCommand\GrantBucketAccessToReadOnlyRoleBigqueryMeta()),
        );
        $command->setMeta($meta);

        $this->grantMainProjectToRegisterExternalBucket($externalAnalyticHubClient, $dataExchange);

        try {
            $handler(
                $this->mainProjectCredentials,
                $command,
                [],
                new RuntimeOptions(),
            );
            $this->fail('Should not be able to register bucket from another region.');
        } catch (InvalidArgumentException $e) {
            $this->assertSame('Listing region "eu" must be the same as linked dataset region "us".', $e->getMessage());
            $this->assertSame(3000, $e->getCode());
            $this->assertSame(false, $e->isRetryable());
        }
    }

    private function prepareTestTable(string $bucketDatabaseName, string $externalTableName): void
    {
        $this->createTestTable($this->externalProjectCredentials, $bucketDatabaseName, $externalTableName);

        // FILL DATA
        $insertGroups = [
            [
                'columns' => '`id`, `name`, `large`',
                'rows' => [
                    "1, 'external', 'data'",
                    "2, 'data from', 'external table'",
                    "3, 'it works', 'awesome !'",
                ],
            ],
        ];
        $this->fillTableWithData(
            $this->externalProjectCredentials,
            $bucketDatabaseName,
            $externalTableName,
            $insertGroups,
        );
    }

    /**
     * @return array{DataExchange, Listing}
     */
    private function prepareExternalBucketForRegistration(
        AnalyticsHubServiceClient $externalAnalyticHubClient,
        string $bucketDatabaseName,
        string $location = BaseCase::DEFAULT_LOCATION,
    ): array {
        $externalCredentials = CredentialsHelper::getCredentialsArray($this->externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_external';
        $formattedParent = $externalAnalyticHubClient->locationName($externalProjectStringId, $location);

        // 1.1 Create exchanger in source project
        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);

        try {
            // delete if exist in case of retry
            $dataExchangeName = AnalyticsHubServiceClient::dataExchangeName(
                $externalProjectStringId,
                $location,
                $dataExchangeId,
            );
            $dataExchange = $externalAnalyticHubClient->getDataExchange($dataExchangeName);
            $externalAnalyticHubClient->deleteDataExchange($dataExchange->getName());
        } catch (Throwable $e) {
            // ignore
        }

        $dataExchange = $externalAnalyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange,
        );

        $listingId = str_replace('-', '_', $externalCredentials['project_id']) . '_listing';
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $externalProjectStringId,
                $bucketDatabaseName,
            ),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);

        // 1.2 Create listing for extern bucket
        $createdListing = $externalAnalyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);
        return [$dataExchange, $createdListing];
    }

    private function grantMainProjectToRegisterExternalBucket(
        AnalyticsHubServiceClient $externalAnalyticHubClient,
        DataExchange $dataExchange,
    ): void {
        $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
        $iamExchangerPolicy = $externalAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $binding = $iamExchangerPolicy->getBindings();
        $binding[] = new Binding([
            'role' => 'roles/analyticshub.admin',
            'members' => ['serviceAccount:' . $mainCredentials['client_email']],
        ]);
        $iamExchangerPolicy->setBindings($binding);
        $externalAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangerPolicy);
    }
}

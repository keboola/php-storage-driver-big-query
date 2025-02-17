<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Exception;
use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\Billing\V1\ProjectBillingInfo;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Service\CloudResourceManager\Project;
use Google\Service\Exception as GoogleServiceException;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\IAMServiceWrapper;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\LogMessage;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo\TableColumn;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use LogicException;
use PHPUnit\Framework\TestCase;
use PHPUnitRetry\RetryTrait;
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\Store\FlockStore;
use Throwable;

class BaseCase extends TestCase
{
    use RetryTrait;

    public const EU_LOCATION = 'EU';
    public const US_LOCATION = 'US';
    public const DEFAULT_LOCATION = self::US_LOCATION;

    protected string $testRunId;

    /**
     * @var array{
     *     0:array{GenericBackendCredentials, CreateProjectResponse, string},
     *     1:array{GenericBackendCredentials, CreateProjectResponse, string},
     *     2:array{GenericBackendCredentials, CreateProjectResponse, string},
     * }
     */
    // @phpstan-ignore-next-line
    protected array $projects = [];

    protected GCPClientManager $clientManager;

    protected ParatestFileLogger $log;

    protected static function getRand(): string
    {
        return substr(md5(uniqid((string) mt_rand(), true)), 0, 3);
    }

    private function initialize(): void
    {
        if (file_exists('/tmp/prj0-cred')) {
            unlink('/tmp/prj0-cred');
        }
        if (file_exists('/tmp/prj0-res')) {
            unlink('/tmp/prj0-res');
        }
        if (file_exists('/tmp/prj0-id')) {
            unlink('/tmp/prj0-id');
        }

        if (file_exists('/tmp/prj1-cred')) {
            unlink('/tmp/prj1-cred');
        }
        if (file_exists('/tmp/prj1-res')) {
            unlink('/tmp/prj1-res');
        }
        if (file_exists('/tmp/prj1-id')) {
            unlink('/tmp/prj1-id');
        }

        if (file_exists('/tmp/prj2-cred')) {
            unlink('/tmp/prj2-cred');
        }
        if (file_exists('/tmp/prj2-res')) {
            unlink('/tmp/prj2-res');
        }
        if (file_exists('/tmp/prj2-id')) {
            unlink('/tmp/prj2-id');
        }

        $this->dropProjects($this->getStackPrefix());
        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $suffix = date('mdHis') . self::getRand();
        $project0 = [
            ...$this->createProject('main-' . $suffix),
            $nameGenerator->createProjectId('main-' . $suffix),
        ];
        $project1 = [
            ...$this->createProject('link-' . $suffix),
            $nameGenerator->createProjectId('link-' . $suffix),
        ];

        $project2 = [
            ...$this->createProject('ex-' . $suffix),
            $nameGenerator->createProjectId('ex-' . $suffix),
        ];
        file_put_contents('/tmp/prj0-cred', $project0[0]->serializeToJsonString());
        file_put_contents('/tmp/prj0-res', $project0[1]->serializeToJsonString());
        file_put_contents('/tmp/prj0-id', $project0[2]);

        file_put_contents('/tmp/prj1-cred', $project1[0]->serializeToJsonString());
        file_put_contents('/tmp/prj1-res', $project1[1]->serializeToJsonString());
        file_put_contents('/tmp/prj1-id', $project1[2]);

        file_put_contents('/tmp/prj2-cred', $project2[0]->serializeToJsonString());
        file_put_contents('/tmp/prj2-res', $project2[1]->serializeToJsonString());
        file_put_contents('/tmp/prj2-id', $project2[2]);
    }

    protected function setUp(): void
    {
        $this->log = new ParatestFileLogger($this->getName(false));
        $this->clientManager = new GCPClientManager($this->log);
        $this->log->setPrefix($this->getTestHash());
        $this->log->add('Starting test: ' . $this->getName());
        $GLOBALS['log'] = $this->log;
        if (!file_exists('/tmp/initialized')) {
            $store = new FlockStore('/tmp/test-lock');
            $factory = new LockFactory($store);
            $lock = $factory->createLock('init');
            $this->log->add('init lock');
            if ($lock->acquire(true)) {
                if (!file_exists('/tmp/initialized')) {
                    $this->log->add('initialize');
                    $this->initialize();
                    touch('/tmp/initialized');
                    $lock->release();
                } else {
                    $this->log->add('initialized');
                }
            } else {
                $this->log->add('not locked');
            }
        } else {
            $this->log->add('already initialized');
        }

        $ghRunId = getenv('BUILD_ID');
        if ($ghRunId === false) {
            $this->testRunId = (string) rand(100000, 999999);
        } else {
            $this->testRunId = (string) $ghRunId;
        }

        $prj0Cred = new GenericBackendCredentials();
        $prj0Cred->mergeFromJsonString((string) file_get_contents('/tmp/prj0-cred'));

        $meta = new Any();
        $meta->pack(
            (new GenericBackendCredentials\BigQueryCredentialsMeta())
                ->setRegion(self::DEFAULT_LOCATION),
        );
        $prj0Cred->setMeta($meta);

        $prj0Res = new CreateProjectResponse();
        $prj0Res->mergeFromJsonString((string) file_get_contents('/tmp/prj0-res'));

        $prj1Cred = new GenericBackendCredentials();
        $prj1Cred->mergeFromJsonString((string) file_get_contents('/tmp/prj1-cred'));
        $prj1Cred->setMeta($meta);
        $prj1Res = new CreateProjectResponse();
        $prj1Res->mergeFromJsonString((string) file_get_contents('/tmp/prj1-res'));

        $prj2Cred = new GenericBackendCredentials();
        $prj2Cred->mergeFromJsonString((string) file_get_contents('/tmp/prj2-cred'));
        $prj2Cred->setMeta($meta);
        $prj2Res = new CreateProjectResponse();
        $prj2Res->mergeFromJsonString((string) file_get_contents('/tmp/prj2-res'));
        $this->projects = [
            [
                $prj0Cred,
                $prj0Res,
                (string) file_get_contents('/tmp/prj0-id'),
            ],
            [
                $prj1Cred,
                $prj1Res,
                (string) file_get_contents('/tmp/prj1-id'),
            ],
            [
                $prj2Cred,
                $prj2Res,
                (string) file_get_contents('/tmp/prj2-id'),
            ],
        ];
    }

    protected function tearDown(): void
    {
        $this->log->add('END of test' . $this->getName());
        $this->log->add($this->getStatusMessage());
        parent::tearDown();
    }

    /**
     * @return array{GenericBackendCredentials, CreateProjectResponse}
     * @throws \Google\ApiCore\ApiException
     * @throws \Google\ApiCore\ValidationException
     * @throws \Keboola\StorageDriver\Shared\Driver\Exception\Exception
     */
    protected function createProject(string $id): array
    {
        $this->log->add('CREATING: ' . $id);

        $handler = new CreateProjectHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new CreateprojectCommand();

        $meta = new Any();
        $meta->pack(
            (new CreateProjectCommand\CreateProjectBigqueryMeta())
                ->setGcsFileBucketName((string) getenv('BQ_BUCKET_NAME')),
        );

        $command->setStackPrefix($this->getStackPrefix());
        $command->setProjectId($id);
        $command->setMeta($meta);

        $response = $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        assert($response instanceof CreateProjectResponse);

        $meta = new Any();
        $meta->pack(
            (new GenericBackendCredentials\BigQueryCredentialsMeta())
                ->setRegion(self::DEFAULT_LOCATION)
                ->setFolderId((string) getenv('BQ_FOLDER_ID')),
        );
        return [
            (new GenericBackendCredentials())
                ->setPrincipal($response->getProjectUserName())
                ->setSecret($response->getProjectPassword())
                ->setMeta($meta),
            $response,
        ];
    }

    public function dropProjects(string $startingWith): void
    {
        $mainClient = $this->clientManager->getProjectClient($this->getCredentials());
        $billingClient = $this->clientManager->getBillingClient($this->getCredentials());
        $storageManager = $this->clientManager->getStorageClient($this->getCredentials());

        $meta = $this->getCredentials()->getMeta();
        if ($meta !== null) {
            // override root user and use other database as root
            $meta = $meta->unpack();
            assert($meta instanceof GenericBackendCredentials\BigQueryCredentialsMeta);
            $folderId = $meta->getFolderId();
        } else {
            throw new Exception('BigQueryCredentialsMeta is required.');
        }

        $parent = $folderId;
        $fileStorageBucketName = (string) getenv('BQ_BUCKET_NAME');
        // Iterate over pages of elements
        $pagedResponse = $mainClient->listProjects('folders/' . $parent);
        $this->log->add('TRY DEL:' . $startingWith);
        foreach ($pagedResponse->iteratePages() as $page) {
            /** @var Project $element */
            foreach ($page as $element) {
                if (str_starts_with($element->getProjectId(), $startingWith)) {
                    $this->log->add('DROPPING:' . $element->getProjectId());
                    $formattedName = $mainClient->projectName($element->getProjectId());
                    $billingInfo = new ProjectBillingInfo();
                    $billingInfo->setBillingEnabled(false);
                    $billingClient->updateProjectBillingInfo($formattedName, ['projectBillingInfo' => $billingInfo]);

                    $fileStorageBucket = $storageManager->bucket($fileStorageBucketName);
                    $fileStorageBucket->iam()->reload();
                    $policy = $fileStorageBucket->iam()->policy();

                    foreach ($policy['bindings'] as $bindingKey => $binding) {
                        if ($binding['role'] === 'roles/storage.objectAdmin') {
                            foreach ($binding['members'] as $key => $member) {
                                if (strpos($member, 'serviceAccount:' . $element->getProjectId()) === 0) {
                                    unset($policy['bindings'][$bindingKey]['members'][$key]);
                                }
                            }
                        }
                    }

                    $fileStorageBucket->iam()->setPolicy($policy);

                    $operationResponse = $mainClient->deleteProject($formattedName);
                    $operationResponse->pollUntilComplete();
                    if (!$operationResponse->operationSucceeded()) {
                        $error = $operationResponse->getError();
                        assert($error !== null);
                        throw new Exception($error->getMessage(), $error->getCode());
                    }
                }
            }
        }
    }

    /**
     * Get credentials from envs
     */
    protected function getCredentials(string $region = BaseCase::DEFAULT_LOCATION): GenericBackendCredentials
    {
        $principal = getenv('BQ_PRINCIPAL');
        if ($principal === false) {
            throw new LogicException('Env "BQ_PRINCIPAL" is empty');
        }

        $secret = getenv('BQ_SECRET');
        if ($secret === false) {
            throw new LogicException('Env "BQ_SECRET" is empty');
        }
        $secret = str_replace("\\n", "\n", $secret);

        $folderId = (string) getenv('BQ_FOLDER_ID');
        if ($folderId === '') {
            throw new LogicException('Env "BQ_FOLDER_ID" is empty');
        }

        $any = new Any();
        $any->pack(
            (new GenericBackendCredentials\BigQueryCredentialsMeta())
                ->setFolderId($folderId)
                ->setRegion($region),
        );
        return (new GenericBackendCredentials())
            ->setPrincipal($principal)
            ->setSecret($secret)
            ->setMeta($any);
    }

    /**
     * @param array<mixed> $expected
     * @param array<mixed> $actual
     */
    protected function assertEqualsArrays(array $expected, array $actual): void
    {
        sort($expected);
        sort($actual);

        $this->assertEquals($expected, $actual);
    }

    protected function getStackPrefix(): string
    {
        $stackPrefix = getenv('BQ_STACK_PREFIX');
        if ($stackPrefix === false) {
            $stackPrefix = 'local';
        }
        return $stackPrefix;
    }

    private function dropTestBucket(
        GenericBackendCredentials $projectCredentials,
        string $bucketId,
        ?string $branchId,
    ): void {
        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $datasetName = $nameGenerator->createObjectNameForBucketInProject($bucketId, $branchId);
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);
        $dataset = $bqClient->dataset($datasetName);
        try {
            $dataset->delete(['deleteContents' => true]);
        } catch (Throwable) {
            // ignore
        }
    }

    protected function createTestBucket(
        GenericBackendCredentials $projectCredentials,
        string $projectId,
        ?string $branchId = null,
    ): CreateBucketResponse {
        $bucket = $this->getTestHash() . 'in.c-Test';
        $this->log->add('Creating bucket:' . $bucket);
        $this->dropTestBucket(
            $projectCredentials,
            $bucket,
            $branchId,
        );
        $handler = new CreateBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($projectId)
            ->setBucketId($bucket);

        $meta = new Any();
        $meta->pack(new CreateBucketCommand\CreateBucketBigqueryMeta());
        $command->setMeta($meta);

        if ($branchId !== null) {
            $command->setBranchId($branchId);
        }

        $response = $handler(
            $projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(CreateBucketResponse::class, $response);

        $bigQueryClient = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);

        $dataset = $bigQueryClient->dataset($response->getCreateBucketObjectName());

        $bucketInfo = $dataset->info();
        $this->assertArrayNotHasKey('defaultTableExpirationMs', $bucketInfo);
        $this->assertInstanceOf(Dataset::class, $dataset);
        $this->assertEquals($response->getCreateBucketObjectName(), $dataset->identity()['datasetId']);
        $this->assertTrue($dataset->exists());
        return $response;
    }

    protected function getGCSClient(): StorageClient
    {
        return new StorageClient(['keyFile' => CredentialsHelper::getCredentialsArray($this->getCredentials())]);
    }

    protected function clearGCSBucketDir(string $bucket, string $prefix): void
    {
        $client = $this->getGCSClient();
        $bucket = $client->bucket($bucket);
        $this->log->add('DELETE OBJECTS BY PREFIX: ' . $prefix);
        $objects = $bucket->objects(['prefix' => $prefix]);
        /** @var StorageObject $object */
        foreach ($objects as $object) {
            $this->log->add('DELETE: ' . $object->name());
            $object->delete();
        }
    }

    protected function getTestHash(): string
    {
        $name = $this->getName();
        // Create a raw binary sha256 hash and base64 encode it.
        $hash = base64_encode(hash('sha256', $name, true));
        // Trim base64 padding characters from the end.
        $hash = rtrim($hash, '=');
        // remove rest of chars
        $hash = preg_replace('/[^A-Za-z0-9 ]/', '', $hash);
        assert($hash !== null);
        return $hash;
    }

    /**
     * @return StorageObject[]
     */
    protected function listGCSFiles(string $bucket, string $prefix): array
    {
        $client = $this->getGCSClient();
        $bucket = $client->bucket($bucket);
        $objects = $bucket->objects(['prefix' => $prefix]);
        return iterator_to_array($objects);
    }

    /**
     * @return array{size: int, files: string[]}
     */
    protected function listFilesSimple(string $bucket, string $prefix): array
    {
        /** @var array{size: int, files: string[]} $result */
        $result = array_reduce(
            $this->listGCSFiles($bucket, $prefix),
            static function (array $agg, StorageObject $file) {
                $agg['size'] += (int) $file->info()['size'];
                $agg['files'][] = $file->name();
                return $agg;
            },
            ['size' => 0, 'files' => []],
        );

        return $result;
    }

    /**
     * This method will not delete IAM roles and polices but they are deleted with whole project each tests run
     */
    private function dropTestWorkspace(GenericBackendCredentials $projectCredentials, string $workspaceId): void
    {
        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $datasetName = $nameGenerator->createWorkspaceObjectNameForWorkspaceId($workspaceId);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);
        $dataset = $bqClient->dataset($datasetName);
        try {
            $dataset->delete(['deleteContents' => true]);
        } catch (Throwable) {
            // ignore
        }
    }

    /**
     * @return array{GenericBackendCredentials, CreateWorkspaceResponse}
     */
    protected function createTestWorkspace(
        GenericBackendCredentials $projectCredentials,
        CreateProjectResponse $projectResponse,
        string $projectId,
    ): array {
        $workspaceId = 'WS' . substr($this->getTestHash(), -7) . self::getRand();
        $this->dropTestWorkspace(
            $projectCredentials,
            $workspaceId,
        );
        $handler = new CreateWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new CreateWorkspaceCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($projectId)
            ->setWorkspaceId($workspaceId)
            ->setProjectReadOnlyRoleName($projectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack(new CreateWorkspaceCommand\CreateWorkspaceBigqueryMeta());
        $command->setMeta($meta);
        $response = $handler(
            $projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);
        $this->log->add('Workspace created: ' . $response->getWorkspaceObjectName());
        $this->log->add('Workspace created: ' . $response->getWorkspacePassword());

        $meta = new Any();
        $meta->pack(
            (new GenericBackendCredentials\BigQueryCredentialsMeta())
                ->setRegion(self::DEFAULT_LOCATION),
        );

        $credentials = (new GenericBackendCredentials())
            ->setHost($projectCredentials->getHost())
            ->setPrincipal($response->getWorkspaceUserName())
            ->setSecret($response->getWorkspacePassword())
            ->setPort($projectCredentials->getPort());
        $credentials->setMeta($meta);

        $this->log->add('New workspace SA is: ' . $response->getWorkspaceUserName());
        return [$credentials, $response];
    }

    protected function createTableFromDefinition(
        GenericBackendCredentials $credentials,
        BigqueryTableDefinition $def,
    ): void {
        $structure = [
            'columns' => [],
        ];
        foreach ($def->getColumnsDefinitions() as $colDef) {
            $structure['columns'][$colDef->getColumnName()] = [
                'type' => $colDef->getColumnDefinition()->getType(),
                'length' => $colDef->getColumnDefinition()->getLength(),
                'nullable' => $colDef->getColumnDefinition()->isNullable(),
            ];
        }

        if ($def->getPrimaryKeysNames() !== []) {
            $structure['primaryKeysNames'] = $def->getPrimaryKeysNames();
        }
        $this->createTable(
            $credentials,
            $def->getSchemaName(),
            $def->getTableName(),
            $structure,
        );
    }

    /**
     * @param array{columns: array<string, array<string, mixed>>, primaryKeysNames?: array<int, string>} $structure
     */
    protected function createTable(
        GenericBackendCredentials $credentials,
        string $databaseName,
        string $tableName,
        array $structure,
    ): void {
        $createTableHandler = new CreateTableHandler($this->clientManager);
        $createTableHandler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $databaseName;

        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        /** @var array{type: string, length: string, nullable: bool} $columnData */
        foreach ($structure['columns'] as $columnName => $columnData) {
            $col = (new TableColumnShared)
                ->setName($columnName)
                ->setType($columnData['type'])
                ->setNullable($columnData['nullable']);

            if (array_key_exists('length', $columnData)) {
                $col->setLength($columnData['length']);
            }
            $columns[] = $col;
        }

        $createTableCommand = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);

        if (array_key_exists('primaryKeysNames', $structure)) {
            $createTableCommand->setPrimaryKeysNames(
                ProtobufHelper::arrayToRepeatedString($structure['primaryKeysNames']),
            );
        }

        $createTableResponse = $createTableHandler(
            $credentials,
            $createTableCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $createTableResponse);
        $this->assertSame(ObjectType::TABLE, $createTableResponse->getObjectType());
    }

    /**
     * @param array{columns: string, rows: array<int, string>}[] $insertGroups
     */
    protected function fillTableWithData(
        GenericBackendCredentials $credentials,
        string $databaseName,
        string $tableName,
        array $insertGroups,
        bool $truncate = false,
    ): void {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);
        if ($truncate) {
            $bqClient->runQuery($bqClient->query(sprintf(
                'TRUNCATE TABLE %s.%s',
                BigqueryQuote::quoteSingleIdentifier($databaseName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
            )));
        }

        foreach ($insertGroups as $insertGroup) {
            $insert = [];
            foreach ($insertGroup['rows'] as $insertRow) {
                $insert[] = sprintf('(%s)', $insertRow);
            }
            $insertSql = sprintf(
                "INSERT INTO %s.%s\n(%s) VALUES\n%s;",
                BigqueryQuote::quoteSingleIdentifier($databaseName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
                $insertGroup['columns'],
                implode(",\n", $insert),
            );
            $bqClient->runQuery($bqClient->query($insertSql));
        }
    }

    public function isTableExists(BigQueryClient $projectBqClient, string $datasetName, string $tableName): bool
    {
        $dataset = $projectBqClient->dataset($datasetName);
        return $dataset->table($tableName)->exists();
    }

    public function isDatabaseExists(BigQueryClient $projectBqClient, string $datasetName): bool
    {
        return $projectBqClient->dataset($datasetName)->exists();
    }

    public function isUserExists(IAMServiceWrapper $iamService, string $workspacePublicCredentialsPart): bool
    {
        /** @var array<string, string> $credentialsArr */
        $credentialsArr = (array) json_decode($workspacePublicCredentialsPart, true, 512, JSON_THROW_ON_ERROR);
        $serviceAccountsService = $iamService->projects_serviceAccounts;

        try {
            $serviceAcc = $serviceAccountsService->get(
                sprintf(
                    'projects/%s/serviceAccounts/%s',
                    $credentialsArr['project_id'],
                    $credentialsArr['client_email'],
                ),
            );
        } catch (GoogleServiceException $e) {
            if ($e->getCode() === 404) {
                return false;
            } else {
                throw $e;
            }
        }

        assert($serviceAcc !== null);
        return true;
    }

    protected function createTestTable(
        GenericBackendCredentials $credentials,
        string $database,
        ?string $tableName = null,
    ): string {
        if ($tableName === null) {
            $tableName = $this->getTestHash() . '_Test_table';
        }

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $database;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setType(Bigquery::TYPE_INTEGER);
        $columns[] = (new TableColumnShared)
            ->setName('name')
            ->setType(Bigquery::TYPE_STRING)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new TableColumnShared)
            ->setName('large')
            ->setType(Bigquery::TYPE_STRING)
            ->setLength('10000');
        $primaryKeysNames = new RepeatedField(GPBType::STRING);
        $primaryKeysNames[] = 'id';
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setPrimaryKeysNames($primaryKeysNames);

        $handler(
            $credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        return $tableName;
    }

    /**
     * @return LogMessage[]
     */
    public function getLogsOfLevel(BaseHandler $handler, int $level): array
    {
        /** @var LogMessage[] $out */
        $out = [];
        foreach ($handler->getMessages() as $message) {
            /** @var LogMessage $message */
            if ($message->getLevel() === $level) {
                $out[] = $message;
            }
        }
        return $out;
    }

    protected function extractColumnFromResponse(ObjectInfoResponse $response, string $columnName): TableColumn
    {
        $checkedColumn = null;
        $tableInfo = $response->getTableInfo();
        assert($tableInfo !== null);
        foreach ($tableInfo->getColumns() as $column) {
            /** @var TableColumn $column */
            if ($column->getName() === $columnName) {
                $checkedColumn = $column;
                break;
            }
        }
        assert($checkedColumn !== null, 'Column not found');
        return $checkedColumn;
    }

    public function regionsProvider(): Generator
    {
        yield [BaseCase::DEFAULT_LOCATION];
        yield [BaseCase::EU_LOCATION];
    }
}

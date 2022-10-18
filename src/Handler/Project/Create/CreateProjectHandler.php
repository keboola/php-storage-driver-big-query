<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Project\Create;

use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Google\Cloud\ResourceManager\V3\Project;
use Google\Cloud\ResourceManager\V3\ProjectsClient;
use Google\Cloud\ServiceUsage\V1\ServiceUsageClient;
use Google\Protobuf\Internal\Message;
use Google\Service\Iam\ServiceAccount;
use Google_Service_CloudResourceManager;
use Google_Service_CloudResourceManager_Binding;
use Google_Service_CloudResourceManager_GetIamPolicyRequest;
use Google_Service_CloudResourceManager_Policy;
use Google_Service_CloudResourceManager_SetIamPolicyRequest;
use Google_Service_Iam;
use Google_Service_Iam_CreateServiceAccountKeyRequest;
use Google_Service_Iam_CreateServiceAccountRequest;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\GCPServiceIds;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;

final class CreateProjectHandler implements DriverCommandHandlerInterface
{
    public const ENABLED_SERVICES_FOR_PROJECT = [
        GCPServiceIds::SERVICE_USAGE_SERVICE,
        GCPServiceIds::IAM_SERVICE,
        GCPServiceIds::BIGQUERY_SERVICE,
    ];

    public const PRIVATE_KEY_TYPE = 'TYPE_GOOGLE_CREDENTIALS_FILE';
    public const KEY_DATA_PROPERTY_PRIVATE_KEY = 'private_key';

    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @throws ValidationException
     * @throws Exception
     * @throws ApiException
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateProjectCommand);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $projectId = $nameGenerator->createProjectId($command->getProjectId());

        $meta = $credentials->getMeta();
        if ($meta !== null) {
            // override root user and use other database as root
            $meta = $meta->unpack();
            assert($meta instanceof GenericBackendCredentials\BigQueryCredentialsMeta);
            $folderId = $meta->getFolderId();
        } else {
            throw new Exception('BigQueryCredentialsMeta is required.');
        }

        $projectsClient = $this->clientManager->getProjectClient($credentials);

        $projectCreateResult = $this->createProject($projectsClient, $folderId, $projectId);
        $projectName = $projectCreateResult->getName();

        $serviceUsageClient = $this->clientManager->getServiceUsageClient($credentials);
        $this->enableServicesForProject($serviceUsageClient, $projectName);

        $projectServiceAccountId = $nameGenerator->createProjectServiceAccountId($command->getProjectId());
        $iAmClient = $this->clientManager->getIamClient($credentials);
        $projectServiceAccount = $this->createServiceAccount($iAmClient, $projectServiceAccountId, $projectName);

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $this->setPermissionsToServiceAccount($cloudResourceManager, $projectName, $projectServiceAccount->getEmail());
        $keyData = $this->createKeyFileCredentials($iAmClient, $projectServiceAccount);

        $privateKey = $keyData[self::KEY_DATA_PROPERTY_PRIVATE_KEY];
        unset($keyData[self::KEY_DATA_PROPERTY_PRIVATE_KEY]);
        $publicPart = json_encode($keyData);
        assert($publicPart !== false);

        return (new CreateProjectResponse())
            ->setProjectUserName($publicPart)
            ->setProjectPassword($privateKey);
    }

    /**
     * @throws ApiException
     * @throws Exception
     * @throws ValidationException
     */
    private function createProject(ProjectsClient $projectsClient, string $folderId, string $projectId): Project
    {
        $project = new Project();

        $project->setParent('folders/' . $folderId);
        $project->setProjectId($projectId);
        $project->setDisplayName($projectId);

        $operationResponse = $projectsClient->createProject($project);
        $operationResponse->pollUntilComplete();
        if ($operationResponse->operationSucceeded()) {
            /** @var Project $projectCreateResult */
            $projectCreateResult = $operationResponse->getResult();
        } else {
            $error = $operationResponse->getError();
            assert($error !== null);
            throw new Exception($error->getMessage(), $error->getCode());
        }

        return $projectCreateResult;
    }

    /**
     * @throws ApiException
     * @throws Exception
     * @throws ValidationException
     */
    private function enableServicesForProject(ServiceUsageClient $serviceUsageClient, string $projectName): void
    {
        $operationResponse = $serviceUsageClient->batchEnableServices([
            'parent' => $projectName,
            'serviceIds' => self::ENABLED_SERVICES_FOR_PROJECT,
        ]);
        $operationResponse->pollUntilComplete();
        if (!$operationResponse->operationSucceeded()) {
            $error = $operationResponse->getError();
            assert($error !== null);
            throw new Exception($error->getMessage(), $error->getCode());
        }
    }

    private function createServiceAccount(Google_Service_Iam $iamService, string $projectServiceAccountId, string $projectName): ServiceAccount
    {
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $createServiceAccountRequest = new Google_Service_Iam_CreateServiceAccountRequest();

        $createServiceAccountRequest->setAccountId($projectServiceAccountId);
        return $serviceAccountsService->create($projectName, $createServiceAccountRequest);
    }

    private function setPermissionsToServiceAccount(Google_Service_CloudResourceManager $cloudResourceManagerClient, string $projectName, string $serviceAccEmail): void
    {
        $getIamPolicyRequest = new Google_Service_CloudResourceManager_GetIamPolicyRequest();
        $actualPolicy = $cloudResourceManagerClient->projects->getIamPolicy($projectName, $getIamPolicyRequest, []);

        $bigQueryDataOwnerBinding = new Google_Service_CloudResourceManager_Binding();
        $bigQueryDataOwnerBinding->setMembers('serviceAccount:' . $serviceAccEmail);
        $bigQueryDataOwnerBinding->setRole(IAmPermissions::ROLES_BIGQUERY_DATA_OWNER);

        $serviceAccountCreatorBinding = new Google_Service_CloudResourceManager_Binding();
        $serviceAccountCreatorBinding->setMembers('serviceAccount:' . $serviceAccEmail);
        $serviceAccountCreatorBinding->setRole(IAmPermissions::ROLES_IAM_SERVICE_ACCOUNT_CREATOR);

        $finalBinding[] = $actualPolicy->getBindings();
        $finalBinding[] = $bigQueryDataOwnerBinding;
        $finalBinding[] = $serviceAccountCreatorBinding;

        $policy = new Google_Service_CloudResourceManager_Policy();
        $policy->setBindings($finalBinding);

        $setIamPolicyRequest = new Google_Service_CloudResourceManager_SetIamPolicyRequest();
        $setIamPolicyRequest->setPolicy($policy);

        $cloudResourceManagerClient->projects->setIamPolicy($projectName, $setIamPolicyRequest);
    }

    private function createKeyFileCredentials(Google_Service_Iam $iamService, ServiceAccount $serviceAccount): array
    {
        $serviceAccKeysService = $iamService->projects_serviceAccounts_keys;
        $createServiceAccountKeyRequest = new Google_Service_Iam_CreateServiceAccountKeyRequest();
        $createServiceAccountKeyRequest->setPrivateKeyType(self::PRIVATE_KEY_TYPE);
        $key = $serviceAccKeysService->create($serviceAccount->getName(), $createServiceAccountKeyRequest);

        $json = base64_decode($key->getPrivateKeyData());
        $keyData = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        if (!is_array($keyData)) {
            throw new \Exception('Project key credentials missing.');
        }

        return $keyData;
    }
}

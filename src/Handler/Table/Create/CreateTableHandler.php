<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Create;

use Google\Cloud\Core\Exception\BadRequestException;
use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\MetaHelper;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\SQLtoRestDatatypeConverter;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

final class CreateTableHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateTableCommand);

        // validate
        assert($command->getPath()->count() === 1, 'CreateTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'CreateTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'CreateTableCommand.columns is required');

        // define columns
        $createTableOptions = [
            'schema' => [
                'fields' => [],
            ],
        ];
        /** @var TableColumnShared $column */
        foreach ($command->getColumns() as $column) {
            // validate
            assert($column->getName() !== '', 'TableColumnShared.name is required');
            assert($column->getType() !== '', 'TableColumnShared.type is required');

            $columnDefinition = new Bigquery($column->getType(), [
                'length' => $column->getLength() === '' ? null : $column->getLength(),
                'nullable' => $column->getNullable(),
                'default' => $column->getDefault() === '' ? null : $column->getDefault(),
            ]);
            $createTableOptions['schema']['fields'][] = SQLtoRestDatatypeConverter::convertColumnToRestFormat(
                new BigqueryColumn($column->getName(), $columnDefinition)
            );
        }

        $meta = MetaHelper::getMetaFromCommand(
            $command,
            CreateTableCommand\BigQueryTableMeta::class
        );
        if ($meta !== null) {
            assert($meta instanceof CreateTableCommand\BigQueryTableMeta);
            if ($meta->getTimePartitioning() !== null) {
                $timePartitioningOptions = [
                    'type' => $meta->getTimePartitioning()->getType(),
                ];
                if ($meta->getTimePartitioning()->getExpirationMs() !== null) {
                    $timePartitioningOptions['expirationMs'] = $meta->getTimePartitioning()->getExpirationMs();
                }
                if ($meta->getTimePartitioning()->getField() !== null) {
                    $timePartitioningOptions['field'] = $meta->getTimePartitioning()->getField();
                }
                $createTableOptions['timePartitioning'] = $timePartitioningOptions;
            }
            if ($meta->getClustering() !== null) {
                $createTableOptions['clustering'] = [
                    'fields' => ProtobufHelper::repeatedStringToArray($meta->getClustering()->getFields()),
                ];
            }
            if ($meta->getRangePartitioning() !== null) {
                assert($meta->getRangePartitioning()->getRange() !== null);
                $createTableOptions['rangePartitioning'] = [
                    'field' => $meta->getRangePartitioning()->getField(),
                    'range' => [
                        'start' => (int) $meta->getRangePartitioning()->getRange()->getStart(),
                        'end' => (int) $meta->getRangePartitioning()->getRange()->getEnd(),
                        'interval' => (int) $meta->getRangePartitioning()->getRange()->getInterval(),
                    ],
                ];
            }
            $createTableOptions['requirePartitionFilter'] = $meta->getRequirePartitionFilter();
        }

        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];
        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        if ($runtimeOptions->getRunId() !== '') {
            $createTableOptions['labels'] = ['run_id' => $runtimeOptions->getRunId(),];
        }

        $dataset = $bqClient->dataset($datasetName);

        try {
            $dataset->createTable(
                $command->getTableName(),
                $createTableOptions,
            );
        } catch (BadRequestException $e) {
            BadTableDefinitionException::handleBadRequestException(
                $e,
                $datasetName,
                $command->getTableName(),
                $createTableOptions,
            );
        }

        return (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType(ObjectType::TABLE)
            ->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $datasetName,
                new BigqueryTableReflection(
                    $bqClient,
                    $datasetName,
                    $command->getTableName()
                )
            ));
    }
}

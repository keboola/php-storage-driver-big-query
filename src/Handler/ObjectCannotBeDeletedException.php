<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class ObjectCannotBeDeletedException extends Exception
{
    public function __construct(
        string $message,
        int $code = self::ERR_VALIDATION,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }
}

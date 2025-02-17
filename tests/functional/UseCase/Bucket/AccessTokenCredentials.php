<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Auth\FetchAuthTokenInterface;

class AccessTokenCredentials implements FetchAuthTokenInterface
{
    private $accessToken;

    public function __construct($accessToken)
    {
        $this->accessToken = $accessToken;
    }

    public function fetchAuthToken(callable $httpHandler = null)
    {
        return ['access_token' => $this->accessToken];
    }

    public function getCacheKey()
    {
        return null;
    }

    public function getLastReceivedToken()
    {
        return ['access_token' => $this->accessToken];
    }
}

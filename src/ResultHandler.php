<?php

namespace Emartech\Chunkulator;

use Throwable;

interface ResultHandler
{
    public function onSuccess(array $requestData): void;
    public function onFailure(array $requestData): void;
    public function onError(array $requestData, Throwable $error): void;
}

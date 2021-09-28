<?php

namespace Emartech\Chunkulator;

use Throwable;

interface ResultHandler
{
    public function onAllChunksDone(array $requestData): void;
    public function onChunkErrorWithNoTriesLeft(string $requestData, Throwable $error): void;
    public function onChunkError(string $requestData, Throwable $error): void;
}

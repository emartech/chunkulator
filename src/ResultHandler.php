<?php

namespace Emartech\Chunkulator;

use Throwable;

interface ResultHandler
{
    public function onAllChunksDone(array $requestData): void;
    public function onChunkErrorWithNoTriesLeft(array $requestData): void;
    public function onChunkError(array $requestData, Throwable $error): void;
}

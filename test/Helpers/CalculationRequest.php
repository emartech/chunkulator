<?php

namespace Emartech\Chunkulator\Test\Helpers;

use Emartech\Chunkulator\Request\ChunkRequest;
use Emartech\Chunkulator\Request\Request;

class CalculationRequest
{
    public static function createChunkRequest($chunkCount, $chunkSize, $chunkId, $tries = Request::MAX_RETRY_COUNT): ChunkRequest
    {
        return new ChunkRequest(
            self::createCalculationRequest($chunkCount, $chunkSize),
            $chunkId,
            $tries
        );
    }

    public static function createCalculationRequest($chunkCount, $chunkSize, $requestId = Constants::TRIGGER_ID): Request
    {
        return new Request(
            $requestId,
            $chunkSize,
            $chunkCount,
            Constants::REQUEST_DATA
        );
    }
}

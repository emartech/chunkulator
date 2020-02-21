<?php

namespace Emartech\Chunkulator\Initiator;

use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Request\ChunkRequest;

class ChunkGenerator
{
    const MAX_RETRY_COUNT = 3;

    /** @return ChunkRequest[] */
    public function createChunks(Request $calculationRequest): array
    {
        $chunks = [];

        for ($chunkId = 0; $chunkId < $calculationRequest->getChunkCount(); $chunkId++) {
            $chunk = new ChunkRequest(
                $calculationRequest,
                $chunkId,
                self::MAX_RETRY_COUNT
            );

            $chunks[] = $chunk;
        }

        return $chunks;
    }
}

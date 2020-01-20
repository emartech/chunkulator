<?php

namespace Emartech\Chunkulator\Initiator;

use Emartech\AmqpWrapper\Queue;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Request\ChunkRequest;

class ChunkGenerator
{
    const MAX_RETRY_COUNT = 3;

    private $queue;

    public function __construct(Queue $queue)
    {
        $this->queue = $queue;
    }

    public function createChunks(Request $calculationRequest)
    {
        for ($chunkId = 0; $chunkId < $calculationRequest->getChunkCount(); $chunkId++) {
            $chunk = new ChunkRequest(
                $calculationRequest,
                $chunkId,
                self::MAX_RETRY_COUNT
            );

            $chunk->enqueueIn($this->queue);
        }
    }
}

<?php

namespace Emartech\Chunkulator\Initiator;

use Emartech\AmqpWrapper\Queue;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Request\ChunkRequest;

class ChunkGenerator
{
    const MAX_RETRY_COUNT = 3;

    private $workerQueue;
    private $notifierQueue;

    public function __construct(Queue $workerQueue, Queue $notifierQueue)
    {
        $this->workerQueue = $workerQueue;
        $this->notifierQueue = $notifierQueue;
    }

    public function createChunks(Request $calculationRequest)
    {
        for ($chunkId = 0; $chunkId < $calculationRequest->getChunkCount(); $chunkId++) {
            $chunk = new ChunkRequest(
                $calculationRequest,
                $chunkId,
                self::MAX_RETRY_COUNT
            );

            $chunk->enqueueIn($this->workerQueue);
        }
    }

    public function createEmptyChunk(Request $calculationRequest)
    {
        $chunk = new ChunkRequest(
            $calculationRequest,
            0,
            self::MAX_RETRY_COUNT
        );

        $chunk->enqueueIn($this->notifierQueue);
    }
}

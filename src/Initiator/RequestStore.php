<?php

namespace Emartech\Chunkulator\Initiator;

use Emartech\AmqpWrapper\Queue;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\ResourceFactory;

class RequestStore
{
    private $chunkSize;
    private $chunkGenerator;
    private $queueFactory;


    public static function calculateChunkCount(int $sourceListCount, int $chunkSize): int
    {
        return $sourceListCount > 0 ? ceil($sourceListCount / $chunkSize) : 1;
    }

    public static function create(int $chunkSize, ResourceFactory $resourceFactory): self
    {
        return new self(
            $chunkSize,
            new ChunkGenerator(),
            $resourceFactory->createQueueFactory()
        );
    }

    public function __construct(int $chunkSize, ChunkGenerator $chunkGenerator, QueueFactory $queueFactory)
    {
        $this->chunkSize = $chunkSize;
        $this->chunkGenerator = $chunkGenerator;
        $this->queueFactory = $queueFactory;
    }

    public function storeRequest(int $sourceListCount, array $requestData, string $requestId): void
    {
        $calculationRequest = new Request(
            $requestId,
            $this->chunkSize,
            self::calculateChunkCount($sourceListCount, $this->chunkSize),
            $requestData
        );

        $queue = $this->createQueue($sourceListCount);

        foreach ($this->chunkGenerator->createChunks($calculationRequest) as $chunk) {
            $chunk->enqueueIn($queue);
        }
    }

    private function createQueue(int $sourceListCount): Queue
    {
        if ($sourceListCount > 0) {
            return $this->queueFactory->createWorkerQueue();
        } else {
            return $this->queueFactory->createNotifierQueue();
        }
    }
}

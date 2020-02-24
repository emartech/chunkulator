<?php

namespace Emartech\Chunkulator\Initiator;

use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\ResourceFactory;
use Interop\Queue\Context;
use Interop\Queue\Queue;

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

        $context = $this->queueFactory->createContext();
        $queue = $this->createQueue($context, $sourceListCount);

        foreach ($this->chunkGenerator->createChunks($calculationRequest) as $chunk) {
            $context->createProducer()->send($queue, $context->createMessage($chunk->toJson()));
        }
    }

    private function createQueue(Context $context, int $sourceListCount): Queue
    {
        if ($sourceListCount > 0) {
            return $this->queueFactory->createWorkerQueue($context);
        } else {
            return $this->queueFactory->createNotifierQueue($context);
        }
    }
}

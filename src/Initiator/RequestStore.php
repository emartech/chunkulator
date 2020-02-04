<?php

namespace Emartech\Chunkulator\Initiator;

use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\ResourceFactory;

class RequestStore
{
    private $chunkCalculator;
    private $chunkGenerator;


    public static function create(int $chunkSize, ResourceFactory $resourceFactory): self
    {
        return new self(
            new ChunkCalculator($chunkSize),
            new ChunkGenerator(
                $resourceFactory->createQueueFactory()->createWorkerQueue(),
                $resourceFactory->createQueueFactory()->createNotifierQueue()
            ),
        );
    }

    public function __construct(ChunkCalculator $chunkCalculator, ChunkGenerator $chunkGenerator)
    {
        $this->chunkCalculator = $chunkCalculator;
        $this->chunkGenerator = $chunkGenerator;
    }

    public function storeRequest(int $sourceListCount, array $requestData, string $triggerId): void
    {
        $chunkCount = $sourceListCount > 0 ? $this->chunkCalculator->calculateChunkCount($sourceListCount) : 1;

        $calculationRequest = new Request(
            $triggerId,
            $this->chunkCalculator->getChunkSize(),
            $chunkCount,
            $requestData
        );

        if ($sourceListCount > 0) {
            $this->chunkGenerator->createChunks($calculationRequest);
        } else {
            $this->chunkGenerator->createEmptyChunk($calculationRequest);
        }
    }
}

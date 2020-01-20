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
            new ChunkGenerator($resourceFactory->createQueueFactory()->createWorkerQueue()),
        );
    }

    public function __construct(ChunkCalculator $chunkCalculator, ChunkGenerator $chunkGenerator)
    {
        $this->chunkCalculator = $chunkCalculator;
        $this->chunkGenerator = $chunkGenerator;
    }

    public function storeRequest(int $sourceListCount, array $requestData, string $triggerId): void
    {
        $calculationRequest = new Request(
            $triggerId,
            $this->chunkCalculator->getChunkSize(),
            $this->chunkCalculator->calculateChunkCount($sourceListCount),
            $requestData
        );

        $this->chunkGenerator->createChunks($calculationRequest);
    }
}

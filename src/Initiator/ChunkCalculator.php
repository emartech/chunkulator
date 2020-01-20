<?php

namespace Emartech\Chunkulator\Initiator;


class ChunkCalculator
{
    private $chunkSize;


    public function __construct(int $chunkSize)
    {
        $this->chunkSize = $chunkSize;
    }

    public function calculateChunkCount(int $sourceListCount): int
    {
        $chunkCount = $sourceListCount / $this->chunkSize;

        if ($chunkCount > (int)$chunkCount) {
            $chunkCount = (int)$chunkCount + 1;
        }

        return $chunkCount;
    }

    public function getChunkSize(): int
    {
        return $this->chunkSize;
    }
}

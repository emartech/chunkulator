<?php

namespace Emartech\Chunkulator\Request;

class ChunkRequest
{
    private $chunkId;
    public $tries;
    private $calculationRequest;


    public function __construct(Request $calculationRequest, int $chunkId, int $tries)
    {
        $this->calculationRequest = $calculationRequest;
        $this->chunkId = $chunkId;
        $this->tries = $tries;
    }

    private function toArray(): array
    {
        return $this->calculationRequest->toArray() + [
            'chunkId' => $this->chunkId,
            'tries' => $this->tries,
        ];
    }

    public function toJson(): string
    {
        return json_encode($this->toArray());
    }

    public function getCalculationRequest(): Request
    {
        return $this->calculationRequest;
    }

    public function getChunkId(): int
    {
        return $this->chunkId;
    }
}

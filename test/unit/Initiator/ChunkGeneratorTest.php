<?php

namespace Emartech\Chunkulator\Test\Unit\Initiator;

use Emartech\Chunkulator\Initiator\ChunkGenerator;
use Emartech\Chunkulator\Request\ChunkRequest;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use PHPUnit\Framework\TestCase;

class ChunkGeneratorTest extends TestCase
{
    /** @var ChunkGenerator */
    private $chunkGenerator;

    protected function setUp(): void
    {
        parent::setUp();

        $this->chunkGenerator = new ChunkGenerator();
    }

    /**
     * @test
     */
    public function createChunks_SourceListEmpty_EmptyChunkCreated()
    {
        $calculationRequest = CalculationRequest::createCalculationRequest(1, 2);

        $chunks = $this->chunkGenerator->createChunks($calculationRequest);

        $this->assertCount(1, $chunks);
        $this->assertChunkForId(0, $chunks[0], $calculationRequest);
    }

    /**
     * @test
     */
    public function createChunks_SingleChunkCalculation_OneChunkCreated()
    {
        $calculationRequest = CalculationRequest::createCalculationRequest(1, 2);

        $chunks = $this->chunkGenerator->createChunks($calculationRequest);

        $this->assertCount(1, $chunks);
        $this->assertChunkForId(0, $chunks[0], $calculationRequest);
    }

    /**
     * @test
     */
    public function createChunks_MultipleChunkCalculation_MoreChunksCreated()
    {
        $calculationRequest = CalculationRequest::createCalculationRequest(2, 2);

        $chunks = $this->chunkGenerator->createChunks($calculationRequest);

        $this->assertCount(2, $chunks);
        $this->assertChunkForId(0, $chunks[0], $calculationRequest);
        $this->assertChunkForId(1, $chunks[1], $calculationRequest);
    }

    private function assertChunkForId(int $chunkId, ChunkRequest $chunk, Request $calculationRequest): void
    {
        $this->assertEquals($chunkId, $chunk->getChunkId());
        $this->assertEquals($calculationRequest, $chunk->getCalculationRequest());
        $this->assertEquals(ChunkGenerator::MAX_RETRY_COUNT, $chunk->tries);
    }
}

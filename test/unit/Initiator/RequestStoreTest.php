<?php

namespace Emartech\Chunkulator\Test\Unit\Initiator;

use Emartech\Chunkulator\Initiator\RequestStore;
use Emartech\Chunkulator\Test\Helpers\Constants;
use PHPUnit\Framework\TestCase;

class RequestStoreTest extends TestCase
{
    /**
     * @test
     * @dataProvider sourceListCountProvider
     */
    public function calculateChunkCount_AllTestCases_Perfect($sourceListCount, $chunkCount)
    {
        $this->assertEquals($chunkCount, RequestStore::calculateChunkCount($sourceListCount, Constants::CHUNK_SIZE));
    }

    public function sourceListCountProvider()
    {
        return [
            'empty -> one chunk' => [
                'sourceListCount' => 0,
                'expected chunk count' => 1,
            ],
            'less than chunk size -> one chunk' => [
                'sourceListCount' => Constants::CHUNK_SIZE - 1,
                'expected chunk count' => 1,
            ],
            'equals chunk size -> one chunk' => [
                'sourceListCount' => Constants::CHUNK_SIZE,
                'expected chunk count' => 1,
            ],
            'chunk size + 1 -> two chunks' => [
                'sourceListCount' => Constants::CHUNK_SIZE + 1,
                'expected chunk count' => 2,
            ],
            '2 * chunk size -> two chunks' => [
                'sourceListCount' => 2 * Constants::CHUNK_SIZE,
                'expected chunk count' => 2,
            ],
        ];
    }
}

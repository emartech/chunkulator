<?php

namespace Emartech\Chunkulator\Test\Unit\Notifier;

use Emartech\AmqpWrapper\Message;
use Emartech\TestHelper\BaseTestCase;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\Exception;
use Emartech\Chunkulator\Notifier\Calculation;
use Emartech\Chunkulator\Notifier\Consumer;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use PHPUnit\Framework\MockObject\Builder\InvocationMocker;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Rule\InvokedCount;

class CalculationTest extends BaseTestCase
{
    /**
     * @var Message|MockObject
     */
    private $message;

    /**
     * @var Consumer|MockObject
     */
    private $consumer;

    /**
     * @var ResultHandler|MockObject
     */
    private $resultHandler;


    protected function setUp(): void
    {
        parent::setUp();
        $this->message = $this->createMock(Message::class);
        $this->consumer = $this->createMock(Consumer::class);
        $this->resultHandler = $this->createMock(ResultHandler::class);
    }

    /**
     * @test
     * @dataProvider chunkProvider
     */
    public function allChunksDone_AllTestCases_ReturnsProperBoolean(int $totalChunks, int $finishedChunks, bool $expected)
    {
        $calculation = new Calculation($this->resultHandler, CalculationRequest::createCalculationRequest($totalChunks, 1));

        for ($i = 0; $i < $finishedChunks; $i++) {
            $calculation->addFinishedChunk($i, $this->message);
        }

        $this->assertEquals($expected, $calculation->allChunksDone());
    }

    public function chunkProvider()
    {
        return [
            'single chunk not finished' => [
                'totalChunks' => 1,
                'finishedChunks' => 0,
                'allChunksDone' => false,
            ],
            'single chunk finished' => [
                'totalChunks' => 1,
                'finishedChunks' => 1,
                'allChunksDone' => true,
            ],
            'more chunks but one is missing' => [
                'totalChunks' => 3,
                'finishedChunks' => 2,
                'allChunksDone' => false,
            ],
            'more chunks all finished' => [
                'totalChunks' => 2,
                'finishedChunks' => 2,
                'allChunksDone' => true,
            ],
        ];
    }

    /**
     * @test
     */
    public function finish_Finished_ResumeTriggeredMessageAcked()
    {
        $this->expectSuccessHandlerCall();

        $this->message->expects($this->once())->method('ack');

        $calculation = new Calculation($this->resultHandler, CalculationRequest::createCalculationRequest(1, 1));
        $calculation->addFinishedChunk(0, $this->message);

        $calculation->finish($this->consumer);
    }

    /**
     * @test
     */
    public function finish_Finished_ResumeFailsMessageRepublished()
    {
        $ex = new Exception();
        $this->expectSuccessHandlerCall()->willThrowException($ex);

        $this->message->expects($this->once())->method('discard');
        $this->message->expects($this->never())->method('publish');

        $calculation = new Calculation($this->resultHandler, CalculationRequest::createCalculationRequest(1, 1));
        $calculation->addFinishedChunk(0, $this->message);

        $this->assertExceptionThrown($this->identicalTo($ex), function () use ($calculation) {
            $calculation->finish($this->consumer);
        });
    }

    private function expectSuccessHandlerCall(InvokedCount $invocationRule = null): InvocationMocker
    {
        return $this->resultHandler->expects($invocationRule ?? $this->once())->method('onSuccess');
    }
}

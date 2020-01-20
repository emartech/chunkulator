<?php

namespace Emartech\Chunkulator\Request;

use Emartech\AmqpWrapper\Message;

class ChunkRequestBuilder
{
    public static function fromMessage(Message $message)
    {
        $messageData = $message->getContents();

        return new ChunkRequest(
            new Request(
                (string)$messageData['triggerId'],
                (int)$messageData['chunkSize'],
                (int)$messageData['chunkCount'],
                $messageData['data']
            ),
            (int)$messageData['chunkId'],
            (int)$messageData['tries']
        );
    }
}

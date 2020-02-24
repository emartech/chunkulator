<?php

namespace Emartech\Chunkulator\Request;

use Interop\Queue\Message;

class ChunkRequestBuilder
{
    public static function fromMessage(Message $message)
    {
        $messageData = json_decode($message->getBody(), true);

        return new ChunkRequest(
            new Request(
                (string)$messageData['requestId'],
                (int)$messageData['chunkSize'],
                (int)$messageData['chunkCount'],
                $messageData['data']
            ),
            (int)$messageData['chunkId'],
            (int)$messageData['tries']
        );
    }
}

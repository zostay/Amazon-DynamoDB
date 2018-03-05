%(
    TableName => tn("Thread"),
    Item => {
        LastPostDateTime => {
            S => "201303190422"
        },
        Tags => {
            SS => ["Update","Multiple Items","HelpMe"]
        },
        ForumName => {
            S => "Amazon DynamoDB"
        },
        Message => {
            S => "I want to update multiple items in a single call. What's the best way to do that?"
        },
        Subject => {
            S => "How do I update multiple items?"
        },
        LastPostedBy => {
            S => "fred@example.com"
        }
    },
    ConditionExpression => "ForumName <> :f and Subject <> :s",
    ExpressionAttributeValues => {
        ':f' => {S => "Amazon DynamoDB"},
        ':s' => {S => "How do I update multiple items?"}
    }
)

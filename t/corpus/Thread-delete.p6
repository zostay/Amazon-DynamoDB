%(
    TableName => tn("Thread"),
    Key => {
        ForumName => {
            S => "Amazon DynamoDB"
        },
        Subject => {
            S => "How do I update multiple items?"
        }
    },
    ConditionExpression => "attribute_not_exists(Replies)",
    ReturnValues => "ALL_OLD"
)

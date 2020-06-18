# Upload Facebook Attachment(async)
The current process sends an attachment to user only when user request the file. This causes two problem:
1. There's a lag time from when user request the file to the time that they receive it (due to the file size)
2. The same file is being sent multiple times when being requested.

## Proposed Approach:
1. Leverage the use of attachment_id by Facebook
2. Create an additional collection in mongo using URL as a primary key.

## Problem Encountered:
1. There are more than 1000 links in our database for each client, to upload each of them synchronously will take a long time. (IO bound)

## Objective:
1. Convert all links to attachment_id provided by Facebook
2. Using asynchronous access

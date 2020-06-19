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

## Tools used:
1. aiohttp
2. aiofiles
3. Python

## Process Breakdown:
1. Get all URLs
2. Filter valid and working URLs
3. Download all URLs (async)
4. Upload to new cloud bucket (blocking)
5. Upload to Facebook for attachment ID (async)
6. Modify database entries

p/s The program's **semaphore** is set at 8, can be increased or lowered depending on the network stability
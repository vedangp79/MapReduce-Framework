# GEN AI P4 Chatbot Feedback

### Example: Describe the bot UI Design that we had to use because of Maizey limitations. {: #Q01}
- [ ] Yum
- [x] Yuk

## Background

### How many ULCS courses has group member 1 taken including the current semester? {: #Q02}
2
### How many ULCS courses has group member 2 taken including the current semester? (0 if you are working alone) {: #Q03}
0 
### How many ULCS courses has group member 3 taken including the current semester? (0 if you are working alone or in a group of 2) {: #Q04}
0

## General
### Approximately how many interactions did you have with the bot on project 4? {: #Q05}
- [ ] 0-3
- [x] 4-10
- [ ] 11-50
- [ ] 51+

### Mark all the topics where the bot was helpful: {: #Q06}
- [ ] Spec questions
- [ ] Design
- [x] Debugging
- [x] Conceptual questions
- [ ] Explaining test cases
- [ ] Setup / toolchain
- [ ] Implementation / code suggestions

### Would a similar bot be helpful for other projects? {: #Q07}
- [x] Yes
- [ ] No
- [ ] Unsure

### Our group used the bot instead of posting on Piazza {: #Q08}
- [ ] Never
- [ ] Rarely
- [x] Sometimes
- [ ] Often
- [ ] Always

### Our group used the bot instead of going to Office Hours {: #Q09}
- [ ] Never
- [ ] Rarely
- [x] Sometimes
- [ ] Often
- [ ] Always

### The bot gave more helpful answers than ChatGPT {: #Q10}
(Reminder: Course policy permits the use of generative AI for projects. https://eecs485.org/syllabus.html#generative-ai-copilot-chatgpt)
- [ ] Never
- [ ] Rarely
- [x] Sometimes
- [ ] Often
- [ ] Always
- [ ] I did not use ChatGPT

### Tell us about your experience using our bot instead of ChatGPT. {: #Q11}
Gave more specific advice sometimes

#### (Optional) Overall, do you have any feedback on the bot? {: #Q12}
Disconnects often

## Interaction 1
### Prompt {: #Q13}
explain the project to me

### Response {: #Q14}
Certainly! Based on the context provided, it seems that you are working on a web development project as part of a course. While the specifics of the project are not outlined here, it involves implementing a distributed system with fault tolerance, and you've mentioned the implementation of shared networking code, a TCP/UDP server and client structure, and a job worker model.

### Was this response helpful? {: #Q15}
- [x] Yes
- [ ] No

### To the best of your knowledge, did it answer your question correctly: {: #Q16}
- [x] Yes
- [ ] No
- [ ] Unsure

### (Optional) Anything else you want to tell us about this interaction? {: #Q17}
No

## Interaction 2
### Prompt {: #Q18}
Can you tell me what this error code tells you:

### Response {: #Q19}
Manager Test 5 Continuously Sending Heartbeats Issue:


As for the Manager sending continuous heartbeats, make sure that the heartbeat loop is correctly predicated on the worker's state and conditions for terminating the server (like shutdown_flag or a similar mechanism) are in place. 


It seems like the heartbeat messages are designed to mimic a worker continually signaling to the Manager that it is still alive. The focus should be that other logic, such as the processing of tasks within the Manager, is not blocked by the code responsible for handling heartbeats. Ensure that you have non-blocking reception of heartbeats or use a separate thread/process for handling them if they are currently interfering with other operations in the single-threaded context.

### Was this response helpful? {: #Q20}
- [x] Yes
- [ ] No

### To the best of your knowledge, did it answer your question correctly: {: #Q21}
- [x] Yes
- [ ] No
- [ ] Unsure

### (Optional) Anything else you want to tell us about this interaction? {: #Q22}
No

## Interaction 3
### Prompt {: #Q23}
Take a look at my listen_for_heartbeats function and give me feedback

### Response {: #Q24}
The listen_for_heartbeats function seems correctly implemented for handling the incoming heartbeat messages by storing the current time (time.time()) for each worker that sent a heartbeat. The issue causing the test failure might not be directly due to this function but rather the way the simulated environment (mocking/stubbing) is interacting with it during testing.


From the pytest logs, specifically the assertion error, it looks like the unit test expected a message to have been sent but it got zero. The reason for this could be multifaceted:



Check if the worker is correctly sending the heartbeat messages.

Verify the network connectivity between worker and manager is indeed established and UDP messages can go through.

Make sure nothing in your test is keeping the worker from being able to send the message.

### Was this response helpful? {: #Q25}
- [x] Yes
- [ ] No

### To the best of your knowledge, did it answer your question correctly: {: #Q26}
- [x] Yes
- [ ] No
- [ ] Unsure

### (Optional) Anything else you want to tell us about this interaction? {: #Q27}
No

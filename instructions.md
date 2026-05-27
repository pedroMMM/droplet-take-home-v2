# Build a Webhook Delivery Service!

Write a program that delivers events to webhook subscribers. The interface, tech stack, and language is up to you. It can be an HTTP API, a CLI harness, or some other interesting idea.

## The Features

Your program should support these features:

- A way to register a new webhook URL.
- A way to ingest an event.
- When an event is ingested, invoke each registered webhook URL.
- A test harness to exercise the program. At minimum, it should generate and register webhook URLs and generate and feed events to your program to deliver to those URLs.
- Observability - logs, metrics, status API, a dashboard, or something totally different. Some way of telling how the system is behaving.

If you'd like to extend or modify these requirements to make your solution better, feel free! You can add constraints around event types, define the semantics, make assumptions about clients or webhook destinations, or other things to show good taste.

A great solution will handle at least some production concerns. Think about retries, persistence, delivery guarantees, authentication, what happens when things fail in weird ways, etc.

# Code Challenges

Use whatever language and tech stack you're most comfortable with.

Spend between two and six hours on this.

Just like in real life, you can use the internet. **Do not** use someone else's solution. Use good judgement. Googling how to read in a file is fine. Googling general approaches to understand the solution space is fine. Googling for someone's solution or just copy-pasting LLM output is not fine.

The goal is to see how you write code and build things. Can you decide on and implement an algorithm? What data structures do you use? Do you have good taste and judgement? Where do you invest time polishing the user experience? How do you organize your code? What parts of the problem seem most important to solve first?

## LLM Use

Droplet uses LLMs to do [great software engineering](https://jamison.dance/02-02-2026/software-engineering-with-llms). You should too! Don't just copy-paste the spec to a robot and submit the output. Your use of LLMs should enable you to get more done and build a better solution. It should be better, faster, show more taste, or in some way benefit users. Use the added capacity the robots provide to produce better technical output and better user outcomes.

**Include the transcript** of your session along with your submission. Good use of AI is a skill. Show off that skill! If you use LLMs and don't include the transcripts we'll assume you didn't read this section carefully enough.

## Rubric

We'll evaluate submissions on the following criteria:

- Does it work?
- Does it demonstrate good taste?
- Does it implement the features you decided to tackle?
- Is it high quality and polished?
- Did you communicate clearly in the README?
- Is the code easy to understand and modify?
- Does it demonstrate great LLM-assisted software engineering?

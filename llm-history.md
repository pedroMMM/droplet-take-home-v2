/ultrawork take the instructions.md and create a claude.md file. This service will use Go, for the test harnes use something out of the box like a HTTP mock (or something that is better).

(heavy human changes) Edit it by hand until it matched what I want.

take the claude.md and create the readme.md

/ultrawork implement what is depicted in the claude.md and readme.md      

/ultrawork implement unit tests. Implement a method to mock time so we can test the dead_letter state       

/ultrawork implement OpenTelemetry to the server, to the HTTP clients, to the DB layer. Introduce Spans in all functions that aren't super basic helpers       

update claude and readme with references to otel

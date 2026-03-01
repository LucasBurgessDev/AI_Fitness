~~1. Improve General UI~~
~~2. Improve starter propmpts to be more day to day relevant like, how was my most recent activity? How is my training going? What should I do to improve my training? How is my recovery today?~~
~~3. Save responses for future reference about my profile and training~~
~~4. Integrate into my Google Calendar to track my training and recovery schedule using MCP. Connect using my email: lucasburgess8@gmail.com~~
~~5. FIX THE CALENDAR INTEGRATION ERROR: Unfortunately, it's still not working. It seems like the Google Calendar API needs to be enabled in the project settings. Please try signing out and signing back in to grant the necessary permissions. Once you do that, I should be able to access your calendar and create training events for you.~~
~~6. Add support for multiple allowed emails in the secret as comma separated list, so I can share access with my coach and training partners.~~
~~7. Make a dark mode for the app~~
~~7.1. IMPORTANT - ensure that lucasburgess8@gmail.com works for login. Right now it's restricted to copmany users only.~~
~~8. Add more detailed analytics and insights based on user data~~

~~9. Improve the chatbots responses to think outside the box and provide more creative and personalized suggestions~~
~~10. Use a newer Gemini model for the chatbot to improve its performance and capabilities~~
~~11. Look at if HRV data can be used to provide insights on recovery and training readiness from our Garmin integration~~
~~12. Look at any other ways to get richer data out of the Garmin integration to provide better insights and suggestions~~
~~13. Using the goals part of the UI give better KPI tracking towards this. Make it a clear aim of the chatbot~~
~~14. Add the ability to add as an app on pixel when opened in Chrome, so I can have it more easily accessible on my phone and use it throughout the day to track my training and recovery.~~
~~15. Add option to rename the sessions in the agent, so I can easily identify them and keep track of different conversations and topics related to my training and recovery. This will help me stay organized and quickly find relevant information when I need it.~~
~~16. Add a feature to allow users to set reminders for their training and recovery activities, which can be sent as notifications to their phone or email.~~
~~17. Getting error failed to fetch~~ (fixed: added --timeout 3600 to Cloud Run service deploy)
~~18. Add a feature to execture the garmin data pull. Ensure concurrany is set to 1 only.~~ (added POST /api/garmin/sync with running-execution guard)
~~19. Improve the UI to make it more app like. Less scroll in window. More like the Gemini app interface.~~ (100dvh, Gemini-style welcome view, suggestion cards in center, iOS safe area)
20. Add response caching to speed up repeated/similar queries.
21. Add a caching layer to the BQ data for the last 30 days data to speed up response times for recent activity queries.
22. Plan in application.md to create a mobile app version of MCP using Flutter, to provide a more seamless and accessible experience for users on their phones. This app would sync with the existing MCP backend and provide all the same features and functionality in a mobile-friendly format. It should be available for both iOS and Android devices, and could be distributed through the App Store and Google Play Store for easy access. The mobile app would allow users to track their training and recovery on the go, receive notifications and reminders, and have a more convenient way to interact with the MCP chatbot throughout their day.
23. Implement streaming responses (SSE) so the first tokens appear in ~1-2s instead of waiting for the full LLM response. Requires: StreamingResponse in app.py /chat, yield partial chunks from ADK runner.run_async event stream in agent.py, EventSource/readable stream fetch in the frontend JS.
24. Create plan in commercial.md to launch as a product. User data needs to be stored securely and privately, with clear terms of service and privacy policy. I'd need to set it up as a free service to begin with. How could I manage Garmin credentials at scale? Not sure if Garmin would allow this at scale, might need to look into their API terms and conditions and possibly reach out to them for partnership or permission. I'd also need to consider the costs of running the service, especially with the Google Cloud services involved, and how to monetize it in the future if it gains traction. Potential monetization strategies could include a premium subscription for advanced features, personalized coaching, or partnerships with fitness brands.
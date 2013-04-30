---
layout: base
title: Titan for App Engine
---

**NOTE: We're moving to GitHub! Pardon our dust as we move things over.**
- fotinakis, 2013-05-01

----

*Titan for App Engine* is a suite of powerful tools that helps make building Python apps on App Engine even easier.

Titan started as a simple filesystem abstraction, but has grown into a fully-featured library that extends the native App Engine APIs and provides reusable modules for many common patterns.

* **Titan Files**: a filesystem abstraction for App Engine.
  * **Titan Versions**: a lightweight version control system for atomically updating files.
  * **Titan Microversions**: automatic versioning of every file change.
  * **Mixins**: simple plugins that let you extend and customize Titan Files.

Also includes libraries which are built on Titan Files:
* **Titan Stats**: Customizable counters for recording app statistics over time.
* **Titan Pipelines**: A data-processing pipeline implementation.
* **Titan Users API**: A convenience wrapper around the App Engine Users API.
* **Titan Tasks**: Granular, realtime feedback over deferred task batches.
* **Titan Channel API**: Centralized, multi-subscriber broadcast channels.


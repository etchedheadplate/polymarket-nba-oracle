This repository is part of a system for technical analysis of a Polymarket NBA data. You can learn more [here](https://sonyn.dev/blog/2026-04-07-a-random-walk-down-diamond-district/).

Oracle service collects Polymarket NBA-related event data (games, markets, and price history) via API, processes it, and stores it in a database. The service can be adapted for processing game events from other sports leagues (e.g. WNBA, NHL, NFL).

## Schema

![schema](demo/schema.png)

## Usage

### Environment

Create a `.env` file in the root of the repository using the `.env.example` template.

### Running

The system is built around a RabbitMQ-based task processing model. A consumer subscribes to task queues, executes incoming tasks, and publishes results back to response queues.

Start RabbitMQ consumer:

```bash
python -m main
```

(Optional) Run manual database update

```bash
python -m src.service.etl.update [--keep-dumps]
```

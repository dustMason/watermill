# Watermill

A tiny proof of concept background worker queue built with Redis streams.

Terminal 0
```
docker-compose up
```

Terminal 1
```
bundle exec ruby server.rb left
```

Terminal 2
```
bundle exec ruby server.rb right
```

Terminal 3
```
bundle exec ruby client.rb
```

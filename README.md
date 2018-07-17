This package holds GCS Utlities 

## logging package

You can create a time based file that rollovers after max age
```
tr := gcs.NewTimeRotator(
		context.Background(),
		"web-logs",
		"message-events",
		"2006/01/20060102-1504",
		uuid.NewV4().String(),
		15*time.Minute,
	)
  ```

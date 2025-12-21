
<img width="3880" height="1296" alt="Synthetis v1 2 0-alpha" src="https://github.com/user-attachments/assets/1cb61bb1-328a-49ef-a7df-541a10dbf3ab" />

<h1  align="center">Synthetis v1.3.0-alpha</h1>


Stess test
```
BenchmarkWriteThroughput-10  |  2355129  |  505.2 ns/op  |  1979385 ops/s  |  8 allocs/op
```

<h2 align="center">Docs</h2>
<h3>Write operation</h3>
It takes 3 params in func body. 

```go
func (db *synthetis.DB) Write(metric  string, labels  map[string]string, value  ...float64) error
```

```metric``` - stands for metric name 

```labels``` - stands for labels creation

```value``` - stands for points creation


```go
if  err  :=  sth.Write("each_millisecond_metric", map[string]string{"user": "pooser"}, 34.00); err  !=  nil {
	slog.Info("Error occured", "err", err)
	return
}
```

<h3>Query operation</h3>

```go
func (db *synthetis.DB) Query(metric  string, labels  map[string]string, timeBefore  int64) ([]entity.SeriesResult, error)
```

```metric``` - stands for metric name 

```labels``` - stands for label creation

```value``` - stands for points creation


```go
res, err  :=  sth.Query("each_millisecond_metric", map[string]string{"user": "pooser"}, 0, 0)

if  err  !=  nil {
	slog.Info("Error occured", "err", err)
	return
}
```

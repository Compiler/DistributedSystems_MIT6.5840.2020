

main: 
	go build -buildmode=plugin mrapps/wc.go
	- rm main/*out*
	go run main/mrmaster.go main/pg-*.txt
	more mr-out-0

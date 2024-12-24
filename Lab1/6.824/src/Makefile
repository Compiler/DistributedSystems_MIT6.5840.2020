

main: 
	go build -buildmode=plugin mrapps/wc.go
	- rm mr-out*
	go run main/mrmaster.go main/pg-*.txt
	more mr-out-0

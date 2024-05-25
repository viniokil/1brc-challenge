package main

import (
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"syscall"
)

func countLines(data []byte) int {
	count := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			count++
		}
	}
	return count
}

func countLines2(data []byte) int {
	count := 0
	return count
}

func countLinesFile(fPath string) {

	// Відкриваємо файл
	file, err := os.Open(fPath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Отримуємо інформацію про файл
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return
	}

	// Використовуємо syscall для створення mmap
	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fmt.Println("Error mmaping the file:", err)
		return
	}
	defer syscall.Munmap(data)

	// Підрахунок рядків
	lineCount := countLines(data)
	fmt.Println("Total lines:", lineCount)
}

func main() {
	pf, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer pf.Close()
	if err := pprof.StartCPUProfile(pf); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	fPath := "../measurements.txt"
	countLinesFile(fPath)

}

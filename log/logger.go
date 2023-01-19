package log

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/spf13/viper"
)

const(
	LogTarget = "os"

	infoPrefix = "INFO (%s)"
	debugPrefix = "DEBUG (%s)"
	errorPrefix = "ERROR (%s)"
	warnPrefix = "WARN (%s)"
	fatalPrefix = "FATAL (%s)"

	debugLevel = 1
	infoLevel = 2
	warnLevel = 3
	errorLevel = 4
	fatalLevel = 5
)

var (
	levelMap = map[string]int{
		"info":infoLevel,
		"debug":debugLevel,
		"warn":warnLevel,
		"error":errorLevel,
		"fatal":fatalLevel,
	}
)

type Logger interface{
	Info(msg string)
	Debug(msg string)
	Warn(msg string)
	Err(msg string)
	Fatal(msg string)
}

type LoggerDI interface{
	NewLogger(key string) Logger
}

type LoggerConf struct{
	Level string `yaml:"level"`
	Target string `yaml:"target"`	
}

func (lc *LoggerConf) GetConf(){
	path,err := os.Getwd()
	if err!=nil{
		panic(err)
	}
	vip:=viper.New()
	vip.AddConfigPath(path+"/config")
	vip.SetConfigName("app")
	vip.SetConfigType("yaml")
	if err := vip.ReadInConfig(); err!=nil{
		panic(err)
	}
	err = vip.UnmarshalKey("log",&lc)
	if err != nil{
		panic(err)
	}
}

func (lc *LoggerConf) NewLogger(key string) Logger{
	if lc == nil{
		panic("log not set")
	}
	level,ok:=levelMap[lc.Level]
	if !ok{
		level=0
	}
	myLevel := level

	var out io.Writer

	switch lc.Target {
	default:
		out = os.Stdout
	}

	return logImpl{
		logging:log.New(out,infoPrefix,log.Default().Flags()),
		key:key,
		myLevel: myLevel,
	}
}

type logImpl struct{
	logging *log.Logger
	key string
	myLevel int
}

func (l logImpl) Info(msg string){
	if l.myLevel > infoLevel{
		return
	}
	l.logging.SetPrefix(fmt.Sprintf(infoPrefix,l.key))
	l.logging.Output(2,msg)
}
func (l logImpl) Debug(msg string){
		if l.myLevel > debugLevel{
		return
	}
	l.logging.SetPrefix(fmt.Sprintf(debugPrefix,l.key))
	l.logging.Output(2,msg)
}
func (l logImpl)Warn(msg string){
		if l.myLevel > warnLevel{
		return
	}
	l.logging.SetPrefix(fmt.Sprintf(warnPrefix,l.key))
	l.logging.Output(2,msg)
}
func (l logImpl)Err(msg string){
		if l.myLevel > errorLevel{
		return
	}
	l.logging.SetPrefix(fmt.Sprintf(errorPrefix,l.key))
	l.logging.Output(2,msg)
}
func (l logImpl)Fatal(msg string){
		if l.myLevel > fatalLevel{
		return
	}
	l.logging.SetPrefix(fmt.Sprintf(fatalPrefix,l.key))
	l.logging.Output(2,msg)
}
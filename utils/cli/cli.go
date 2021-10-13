// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"text/template"

	"github.com/dolthub/fuzzer/utils/argparser"
)

var (
	CliOut       = os.Stdout
	CliErr       = os.Stderr
	outputClosed uint64
	CliFormat    = docFormat{"<", ">", "", ""}
)

type UsagePrinter func()

type docFormat struct {
	LessThan      string
	GreaterThan   string
	EmphasisLeft  string
	EmphasisRight string
}

// CommandDocumentation is a struct that represents all the data structures required to create the documentation for a command.
type CommandDocumentation struct {
	// The command/sub-command string passed to a command by the caller
	CommandStr string
	// The short description of the command
	ShortDesc string
	// The long description of the command
	LongDesc string
	// The synopsis, an array of strings showing how to use the command
	Synopsis []string
	// A structure that
	ArgParser *argparser.ArgParser
}

// This type is to store the content of a documented command, elsewhere we can transform this struct into
// other structs that are used to generate documentation at the command line and in markdown files.
type CommandDocumentationContent struct {
	ShortDesc string
	LongDesc  string
	Synopsis  []string
}

// Returns the ShortDesc field of the receiver CommandDocumentation with the passed DocFormat injected into the template
func (cmdDoc CommandDocumentation) GetShortDesc() string {
	return cmdDoc.ShortDesc
}

// Returns the LongDesc field of the receiver CommandDocumentation with the passed DocFormat injected into the template
func (cmdDoc CommandDocumentation) GetLongDesc(format docFormat) (string, error) {
	return templateDocStringHelper(cmdDoc.LongDesc, format)
}

// Creates a CommandDocumentation given command string, arg parser, and a CommandDocumentationContent
func GetCommandDocumentation(commandStr string, cmdDoc CommandDocumentationContent, argParser *argparser.ArgParser) CommandDocumentation {
	return CommandDocumentation{
		CommandStr: commandStr,
		ShortDesc:  cmdDoc.ShortDesc,
		LongDesc:   cmdDoc.LongDesc,
		Synopsis:   cmdDoc.Synopsis,
		ArgParser:  argParser,
	}
}

func templateDocStringHelper(docString string, docFormat docFormat) (string, error) {
	templ, err := template.New("description").Parse(docString)
	if err != nil {
		return "", err
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, docFormat); err != nil {
		return "", err
	}
	return templBuffer.String(), nil
}

// Returns the synopsis iterating over each element and injecting the supplied DocFormat
func (cmdDoc CommandDocumentation) GetSynopsis(format docFormat) ([]string, error) {
	lines := cmdDoc.Synopsis
	for i, line := range lines {
		formatted, err := templateDocStringHelper(line, format)
		if err != nil {
			return []string{}, err
		}
		lines[i] = formatted
	}

	return lines, nil
}

func CloseOutput() {
	if atomic.CompareAndSwapUint64(&outputClosed, 0, 1) {
		fmt.Fprintln(CliOut)
	}
}

// ParseArgsOrDie is used for CLI command that should exit after erroring.
func ParseArgsOrDie(ap *argparser.ArgParser, args []string, usagePrinter UsagePrinter) *argparser.ArgParseResults {
	apr, err := ap.Parse(args)

	if err != nil {
		if err != argparser.ErrHelp {
			PrintErrln(err.Error())

			if usagePrinter != nil {
				usagePrinter()
			}

			os.Exit(1)
		}

		// --help param
		if usagePrinter != nil {
			usagePrinter()
		}
		os.Exit(0)
	}

	return apr
}

func HelpAndUsagePrinters(cmdDoc CommandDocumentation) (UsagePrinter, UsagePrinter) {
	// TODO handle error states
	longDesc, _ := cmdDoc.GetLongDesc(CliFormat)
	synopsis, _ := cmdDoc.GetSynopsis(CliFormat)

	return func() {
			PrintHelpText(cmdDoc.CommandStr, cmdDoc.GetShortDesc(), longDesc, synopsis, cmdDoc.ArgParser)
		}, func() {
			PrintUsage(cmdDoc.CommandStr, synopsis, cmdDoc.ArgParser)
		}
}

func PrintHelpText(commandStr, shortDesc, longDesc string, synopsis []string, parser *argparser.ArgParser) {
	_, termWidth := terminalSize()

	indent := "\t"
	helpWidth := termWidth - 10
	if helpWidth < 30 {
		helpWidth = 120
	}

	Println("NAME")
	Printf("%s%s - %s\n", indent, commandStr, shortDesc)

	if len(synopsis) > 0 {
		Println()
		Println("SYNOPSIS")

		for _, curr := range synopsis {
			Printf(indent+"%s %s\n", commandStr, curr)
		}
	}

	Println()
	Println("DESCRIPTION")
	Println(ToIndentedParagraph(longDesc, indent, helpWidth))

	if len(parser.Supported) > 0 || len(parser.ArgListHelp) > 0 {
		Println()
		Println("OPTIONS")
		optionHelp := OptionsUsage(parser, indent, helpWidth)
		Println(optionHelp)
	}
}

func PrintUsage(commandStr string, synopsis []string, parser *argparser.ArgParser) {
	_, termWidth := terminalSize()

	helpWidth := termWidth - 10
	if helpWidth < 30 {
		helpWidth = 120
	}

	if len(synopsis) > 0 {
		for i, curr := range synopsis {
			if i == 0 {
				Println("usage:", commandStr, curr)
			} else {
				Println("   or:", commandStr, curr)
			}
		}
	}

	if len(parser.Supported) > 0 || len(parser.ArgListHelp) > 0 {
		Println()
		Println("Specific", commandStr, "options")
		optionHelp := OptionsUsage(parser, "    ", helpWidth)
		Println(optionHelp)
	}
}

func Println(a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprintln(CliOut, a...)
}

func Print(a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprint(CliOut, a...)
}

func Printf(format string, a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprintf(CliOut, format, a...)
}

func PrintErrln(a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprintln(CliErr, a...)
}

func PrintErr(a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprint(CliErr, a...)
}

func PrintErrf(format string, a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprintf(CliErr, format, a...)
}

func OptionsUsage(ap *argparser.ArgParser, indent string, lineLen int) string {
	var lines []string

	for _, kvTuple := range ap.ArgListHelp {
		k, v := kvTuple[0], kvTuple[1]
		lines = append(lines, "<"+k+">")
		l, err := templateDocStringHelper(v, CliFormat)
		if err != nil {
			panic(err)
		}
		descLines := toParagraphLines(l, lineLen)
		descLines = indentLines(descLines, "  ")
		descLines = append(descLines, "")

		lines = append(lines, descLines...)
	}

	for _, supOpt := range ap.Supported {
		argHelpFmt := "--%[2]s"

		if supOpt.Abbrev != "" && supOpt.ValDesc != "" {
			argHelpFmt = "-%[1]s <%[3]s>, --%[2]s=<%[3]s>"
		} else if supOpt.Abbrev != "" {
			argHelpFmt = "-%[1]s, --%[2]s"
		} else if supOpt.ValDesc != "" {
			argHelpFmt = "--%[2]s=<%[3]s>"
		}

		lines = append(lines, fmt.Sprintf(argHelpFmt, supOpt.Abbrev, supOpt.Name, supOpt.ValDesc))

		l, err := templateDocStringHelper(supOpt.Desc, CliFormat)
		if err != nil {
			panic(err)
		}
		descLines := toParagraphLines(l, lineLen)
		descLines = indentLines(descLines, "  ")
		descLines = append(descLines, "")

		lines = append(lines, descLines...)
	}

	lines = indentLines(lines, indent)
	return strings.Join(lines, "\n")
}

func ToIndentedParagraph(inStr, indent string, lineLen int) string {
	lines := toParagraphLines(inStr, lineLen)
	indentedLines := indentLines(lines, indent)
	joined := strings.Join(indentedLines, "\n")
	return joined
}

// mapStrings iterates over a slice of strings calling the mapping function for each value
// in the slice.  The mapped values are returned in a new slice, and their order corresponds
// with the input slice (The Nth item in the output slice is the result returned by the mapping
// function when given the Nth item from the input slice.)
func mapStrings(strings []string, mapFunc func(string) string) []string {
	if strings == nil {
		return nil
	}

	results := make([]string, len(strings))

	for i, str := range strings {
		results[i] = mapFunc(str)
	}

	return results
}

func toParagraphLines(inStr string, lineLen int) []string {
	var lines []string
	descLines := strings.Split(inStr, "\n")
	for _, descLine := range descLines {
		if len(descLine) == 0 {
			lines = append(lines, "")
		} else {
			lineIndent := ""
			for len(descLine) > 0 && (descLine[0] == ' ' || descLine[0] == '\t') {
				lineIndent += string(descLine[0])
				descLine = descLine[1:]
			}

			descLineLen := lineLen - len(lineIndent)
			for remaining := descLine; len(remaining) > 0; {
				if len(remaining) > descLineLen {
					whiteSpIdx := strings.LastIndexAny(remaining[:descLineLen], " \t")

					splitPt := whiteSpIdx
					if splitPt == -1 {
						splitPt = descLineLen
					}

					line := lineIndent + remaining[:splitPt]
					lines = append(lines, line)

					remaining = remaining[splitPt+1:]
				} else {
					lines = append(lines, lineIndent+remaining)
					remaining = ""
				}
			}
		}
	}

	return lines
}

func indentLines(lines []string, indentation string) []string {
	return mapStrings(lines, func(s string) string {
		return indentation + s
	})
}

func outputIsClosed() bool {
	isClosed := atomic.LoadUint64(&outputClosed)
	return isClosed == 1
}

func terminalSize() (width, height int) {
	defer func() {
		recover()
	}()

	height = -1
	width = -1

	cmd := exec.Command("stty", "size")
	cmd.Stdin = os.Stdin
	out, err := cmd.Output()

	if err == nil {
		outStr := string(out)
		tokens := strings.Split(outStr, " ")
		tempWidth, err := strconv.ParseInt(strings.TrimSpace(tokens[0]), 10, 32)

		if err == nil {
			tempHeight, err := strconv.ParseInt(strings.TrimSpace(tokens[1]), 10, 32)

			if err == nil {
				width, height = int(tempWidth), int(tempHeight)
			}
		}
	}

	return
}

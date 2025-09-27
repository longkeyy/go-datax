package transformer

import (
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
)

// TransformerExecutionParas 转换器执行参数
type TransformerExecutionParas struct {
	columnIndex   *int
	paras         []interface{}
	code          string
	extraPackage  []string
	context       map[string]interface{}
}

func NewTransformerExecutionParas() *TransformerExecutionParas {
	return &TransformerExecutionParas{
		context: make(map[string]interface{}),
	}
}

func (tep *TransformerExecutionParas) SetColumnIndex(index int) {
	tep.columnIndex = &index
}

func (tep *TransformerExecutionParas) GetColumnIndex() *int {
	return tep.columnIndex
}

func (tep *TransformerExecutionParas) SetParas(paras []interface{}) {
	tep.paras = paras
}

func (tep *TransformerExecutionParas) GetParas() []interface{} {
	return tep.paras
}

func (tep *TransformerExecutionParas) SetCode(code string) {
	tep.code = code
}

func (tep *TransformerExecutionParas) GetCode() string {
	return tep.code
}

func (tep *TransformerExecutionParas) SetExtraPackage(packages []string) {
	tep.extraPackage = packages
}

func (tep *TransformerExecutionParas) GetExtraPackage() []string {
	return tep.extraPackage
}

func (tep *TransformerExecutionParas) GetContext() map[string]interface{} {
	return tep.context
}

// TransformerExecution 转换器执行器
type TransformerExecution struct {
	transformerInfo *TransformerInfo
	executionParas  *TransformerExecutionParas
	finalParas      []interface{}
	isChecked       bool

	// 统计信息
	successRecords int64
	failedRecords  int64
	filterRecords  int64
}

func NewTransformerExecution(info *TransformerInfo, paras *TransformerExecutionParas) *TransformerExecution {
	return &TransformerExecution{
		transformerInfo: info,
		executionParas:  paras,
		isChecked:       false,
	}
}

// GenFinalParas 生成最终参数
func (te *TransformerExecution) GenFinalParas() {
	transformerName := te.transformerInfo.GetTransformer().GetTransformerName()

	// 特殊处理groovy转换器
	if transformerName == "dx_groovy" {
		te.finalParas = []interface{}{
			te.executionParas.GetCode(),
			te.executionParas.GetExtraPackage(),
		}
		return
	}

	// 处理其他转换器
	var finalParas []interface{}

	// 如果有columnIndex，作为第一个参数
	if te.executionParas.GetColumnIndex() != nil {
		finalParas = append(finalParas, *te.executionParas.GetColumnIndex())

		// 添加其他参数
		if te.executionParas.GetParas() != nil {
			finalParas = append(finalParas, te.executionParas.GetParas()...)
		}
	} else {
		// 没有columnIndex，直接使用参数
		if te.executionParas.GetParas() != nil {
			finalParas = te.executionParas.GetParas()
		}
	}

	te.finalParas = finalParas
}

// GetFinalParas 获取最终参数
func (te *TransformerExecution) GetFinalParas() []interface{} {
	return te.finalParas
}

// Execute 执行转换
func (te *TransformerExecution) Execute(record element.Record) (element.Record, error) {
	if !te.isChecked {
		te.GenFinalParas()
		te.isChecked = true
	}

	result, err := te.transformerInfo.GetTransformer().Evaluate(record, te.finalParas...)
	if err != nil {
		te.failedRecords++
		return nil, err
	}

	if result == nil {
		te.filterRecords++
	} else {
		te.successRecords++
	}

	return result, nil
}

// 统计信息获取方法
func (te *TransformerExecution) GetSuccessRecords() int64 {
	return te.successRecords
}

func (te *TransformerExecution) GetFailedRecords() int64 {
	return te.failedRecords
}

func (te *TransformerExecution) GetFilterRecords() int64 {
	return te.filterRecords
}

func (te *TransformerExecution) GetTransformerName() string {
	return te.transformerInfo.GetTransformer().GetTransformerName()
}

// BuildTransformerExecutions 从配置构建转换器执行列表
func BuildTransformerExecutions(transformerConfigs []*config.Configuration) ([]*TransformerExecution, error) {
	var executions []*TransformerExecution

	for _, transformerConfig := range transformerConfigs {
		name := transformerConfig.GetString("name")
		if name == "" {
			continue
		}

		// 获取转换器
		transformerInfo, err := GetTransformer(name)
		if err != nil {
			return nil, err
		}

		// 构建执行参数
		paras := NewTransformerExecutionParas()

		// 解析参数
		paramConfig := transformerConfig.GetConfiguration("parameter")
		if paramConfig != nil {
			// columnIndex
			if paramConfig.IsExists("columnIndex") {
				columnIndex := paramConfig.GetInt("columnIndex")
				paras.SetColumnIndex(columnIndex)
			}

			// paras数组
			if paramConfig.IsExists("paras") {
				parasArray := paramConfig.GetList("paras")
				var parasInterface []interface{}
				for _, p := range parasArray {
					parasInterface = append(parasInterface, p)
				}
				paras.SetParas(parasInterface)
			}

			// code（用于groovy）
			if paramConfig.IsExists("code") {
				paras.SetCode(paramConfig.GetString("code"))
			}

			// extraPackage（用于groovy）
			if paramConfig.IsExists("extraPackage") {
				extraPackages := paramConfig.GetStringArray("extraPackage")
				paras.SetExtraPackage(extraPackages)
			}
		}

		// 创建执行器
		execution := NewTransformerExecution(transformerInfo, paras)
		executions = append(executions, execution)
	}

	return executions, nil
}
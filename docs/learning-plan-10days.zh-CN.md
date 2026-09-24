# 10 天读懂 FrontierAgent：Python 零基础学习计划

> 目标：**读懂**这个项目，不要求会写。每天约 6–8 小时。
> 原则：Python 语法只学"项目里真的用到的"，每学一个知识点，立刻去项目里找到它的真实例子。

---

## 0. 先认清规模，定好策略

这个仓库有约 **15 万行 Python**，一行一行读完不现实，也没必要。按下面的优先级分配精力：

| 优先级 | 目录 | 行数量级 | 策略 |
|---|---|---|---|
| ★★★ 精读 | `frontier_agent/core/` | ~1 万 | 框架内核，Agent loop（智能体循环）就在这里，必须逐行读懂 |
| ★★★ 精读 | `plugins/tools/__init__.py` + 几个简单工具 | ~2 千 | 理解"工具"是怎么被定义、注册、调用的 |
| ★★★ 精读 | `workflows/stateful_react_agent/` | ~3 千 | 最简单的完整工作流，是整个项目的"主线样例" |
| ★★ 通读 | `workflows/agent_team/`、`frontier_agent/components/agent_bus/` | ~1 万 | 多 agent 协作，理解数据流和关键类即可 |
| ★★ 通读 | `apodex/`（CLI + TUI） | ~2 万 | 从 `python -m apodex` 入口一路跟到 `run_agent_loop` |
| ★★ 通读 | `frontier_agent/infra/`、`components/observers/` | ~1 万 | LLM 客户端、重试、各种 observer（观察者） |
| ★ 浏览 | `plugins/tools/_sandbox.py`、`_bash_policy.py` 等 | ~1 万 | 知道它负责什么、入口函数是什么，细节按需查 |
| ★ 浏览 | `benchmarks/`、`deploy/`、`scripts/`、`docker/` | ~6 万 | 知道干什么用即可；`frontier_search_bench/.../scorers/query_*` 这类几十个相似文件看一个就够 |

**"读懂"的标准**：对任何一个 ★★★ 文件，你能说清楚：
1. 它对外暴露什么（类/函数）；
2. 谁调用它、它调用谁；
3. 一次正常执行时数据是怎么流过它的。

---

## 1. 准备工作（Day 1 上午前完成，约 1 小时）

```bash
# 安装 uv（项目用的包管理器），然后在仓库根目录：
uv sync --python 3.12 --extra dev

# 验证环境：跑一遍测试（不需要任何 API key）
# 注意：必须先 uv sync，否则会报 ModuleNotFoundError: No module named 'plugins'
uv run pytest tests/test_tool_registry.py -q
```

工具推荐：
- **VS Code + Python/Pylance 插件**：最重要的功能是 `F12` 跳转到定义、`Shift+F12` 查找所有引用。读大项目 80% 的时间都在跳转。
- **`uv run python`** 打开交互式解释器（REPL），随时 `import` 项目里的模块试一试。
- **`breakpoint()`**：在任意一行插入它，再跑测试，就能停在那里查看变量（`p 变量名` 打印，`n` 下一行，`s` 进入函数，`c` 继续）。这是"读懂"代码最快的方式，读完记得删掉。
- 测试文件（`tests/`、`apodex/tests/`）就是**可执行的说明书**：看不懂某个模块时，先读它的测试。

---

## Day 1：Python 基础语法 + 项目全景

### 上午：Python 核心语法（快速过）
只需要能"认出来"，不需要背：
- 变量、`int / float / str / bool / None`
- 容器：`list`、`dict`、`tuple`、`set`，以及切片 `a[1:3]`
- 流程：`if / elif / else`、`for`、`while`、`break / continue`
- 函数：`def`、默认参数、关键字参数、**`*args` / `**kwargs`**、仅限关键字参数（参数列表里单独的 `*`）
- 推导式：`[x for x in xs if ...]`、`{k: v for ...}`（项目里大量使用）
- f-string：`f"hello {name}"`
- 异常：`try / except / finally`、`raise`

推荐资料：Python 官方教程（docs.python.org/zh-cn/3.12/tutorial）第 3–8 章，挑读。

### 下午：模块、包、import
- 一个 `.py` 文件是一个模块，带 `__init__.py` 的目录是一个包
- `import a.b.c`、`from a.b import c`、`as` 别名
- `if __name__ == "__main__":` 的含义
- `python -m 包名` 为什么会执行 `包名/__main__.py`

**项目对照阅读：**
- `README.md`（全文）、`docs/framework.md`（全文）、`docs/README.md`
- `pyproject.toml`：看懂 `dependencies`（依赖）、`[project.scripts]`（`frontier-agent` 命令映射到 `apodex.cli:main`）
- `apodex/__main__.py`（8 行）：理解 `python -m apodex` 是怎么启动的
- `plugins/tools/__init__.py`：看 import 语句，感受"包"的组织方式

**自测：**
- 在终端输入 `frontier-agent`，Python 最终执行的是哪个文件的哪个函数？
- `frontier_agent/`、`plugins/`、`workflows/`、`apodex/`、`benchmarks/` 各自负责什么？谁依赖谁？

---

## Day 2：函数进阶、类与面向对象

### Python 知识点
- 类：`class`、`__init__`、`self`、实例属性 vs 类属性
- 继承与 `super()`（项目里有 40+ 处）
- 特殊方法：`__len__`、`__contains__`、`__repr__`（让对象支持 `len(x)`、`in` 等）
- `@property`、`@staticmethod`、`@classmethod`
- 函数是"一等公民"：函数可以当参数传、当返回值、存进列表
- `lambda` 匿名函数（项目里 200+ 处）
- **装饰器**：`@xxx` 本质就是 `func = xxx(func)`

### 项目对照阅读
- `plugins/tools/__init__.py` 的 `ToolRegistry` 类：一个很标准的小类，读懂每个方法，特别是 `__len__` 和 `__contains__`
- `apodex/cli.py` 中的 `_EngineLogRouter`：继承 `logging.Handler` 并重写 `emit`，典型的继承用法
- `frontier_agent/core/runtime/registries/services.py`：一个极简的"服务注册表"（依赖注入容器），用一个模块级 `dict` 按类型存取对象

**自测：**
- `ToolRegistry.get_for_role` 里为什么把 `import` 写在函数内部？（提示：避免循环导入 / 延迟加载）
- 用自己的话解释：装饰器 `@tool` 放在 `async def glob_search(...)` 上面，执行后 `glob_search` 这个名字指向的是什么？（今天先猜，Day 4 验证）

---

## Day 3：类型注解与数据建模

这个项目**类型注解极其密集**，读不懂注解就读不懂函数签名。今天是关键的一天。

### Python 知识点
- 基础注解：`x: int`、`def f(a: str) -> bool:`
- `list[str]`、`dict[str, Any]`、`tuple[int, ...]`
- `X | None`（可以为空）、`Union`、`Optional`
- `Any`、`Literal["a", "b"]`、`Callable[[int], str]`、`Awaitable`
- `from __future__ import annotations`：几乎每个文件第一行都有，作用是让注解延迟求值
- `TYPE_CHECKING`：只给类型检查器看的 import，运行时不执行
- **`@dataclass`**（80 处）：自动生成 `__init__` 的数据类；`field(default_factory=dict)` 的用法
- **`TypedDict`**：给 dict 声明结构（项目的消息格式就是它）
- **`Protocol`**（58 处）：结构化类型——"只要长得像鸭子就是鸭子"，不需要继承
- **pydantic `BaseModel`**（35 处）：带运行时校验的数据模型
- `Enum` 枚举
- Python 3.12 新泛型语法：`def get[T](service_type: type[T]) -> T:`（见 `services.py`）

### 项目对照阅读
- `frontier_agent/core/messages.py`：`ToolCall`、`Message` 两个 `TypedDict`，以及 `system_msg / user_msg / assistant_msg / tool_msg` 构造函数。**这是和 LLM 对话的数据格式，一定要吃透。**
- `frontier_agent/core/loop_types.py`：`LoopConfig`、`ToolResult`、`Intervention`、`AgentLoopResult` 等 dataclass，以及 `BaseObserver`
- `frontier_agent/core/protocols.py`：`EventSink`、`TraceSink` 等 Protocol
- `frontier_agent/models/pipeline_spec.py`：pydantic 定义的 `PipelineSpec`、`NodeDefinition`
- `frontier_agent/models/` 下其它小文件（都很短）

**自测：**
- 一条 "assistant 调用了工具" 的消息，作为 `Message` 长什么样？对应的工具返回消息又长什么样？
- `Protocol` 和普通基类继承有什么区别？项目为什么在 `protocols.py` 里用 `Protocol`？
- `dataclass` 和 pydantic `BaseModel` 各在什么场景用？

---

## Day 4：工具（Tool）是怎么造出来的——内省与 JSON Schema

### Python 知识点
- `inspect.signature`：运行时读取函数的参数列表
- `typing.get_type_hints`、`get_origin`、`get_args`：运行时读取类型注解
- 函数的 `__doc__`（docstring）、`__name__`
- `@overload`：同一个函数的多种调用形式（只给类型检查器看）
- 海象运算符 `:=`（`if (spec := d.get("x")) is not None:`）
- `getattr(obj, "name", default)`、`callable()`、`isinstance()`
- 正则表达式 `re`（项目里 `re.compile` 239 处）：学会读 `\s`、`\d`、`+`、`*`、`?`、分组 `(...)`
- `json.dumps / json.loads`

### 项目对照阅读（精读）
- **`frontier_agent/core/tool.py`（281 行，逐行读）**：
  - `Tool` dataclass：一个工具 = 名字 + 描述 + JSON schema 参数 + 异步函数
  - `_schema_for_type`：把 Python 类型注解翻译成 JSON Schema
  - `_parse_docstring`：从 docstring 里提取参数说明
  - `_infer_parameters` 与 `tool` 装饰器
- `plugins/tools/glob_search.py`、`grep_search.py`、`read_file.py`：看一个真实的 `@tool` 工具长什么样
- `tests/test_tool_registry.py`

**动手验证（只读，不写业务代码）：**
```bash
uv run python
>>> from plugins.tools import get_builtin_tools
>>> tools = get_builtin_tools()
>>> list(tools)
>>> import json; print(json.dumps(tools["glob_search"].to_openai_schema(), indent=2))
```
对照 `glob_search` 的函数签名和 docstring，看每个字段是从哪里来的。

**自测：**
- LLM 是怎么"知道"有哪些工具、每个工具要什么参数的？
- 为什么 `pyproject.toml` 里的注释说不能开启 ruff 的 `TC` 规则？（答案就在今天读的 `_infer_parameters` 里）

---

## Day 5：异步编程 asyncio——全项目最重要的一块

项目里有 **560+ 个 `async def`、近 1000 个 `await`**。不懂 asyncio 就读不懂任何核心代码。今天全天攻这个。

### Python 知识点（按顺序学）
1. 为什么需要异步：等网络（调 LLM、抓网页）时不想干等
2. `async def` 定义协程函数，`await` 等待结果；`asyncio.run(main())` 启动
3. `asyncio.create_task`：让多个任务并发跑（15 处）
4. `asyncio.gather`：同时等多个任务（14 处）
5. `asyncio.wait_for` / 超时、`asyncio.CancelledError` 取消机制
6. `asyncio.Queue`、`asyncio.Event`、`asyncio.Lock`
7. `async with`（172 处）、`async for`（异步迭代器 / 流式输出）
8. 生成器 `yield` → 异步生成器 `async def ... yield`（LLM 流式输出就是这个）
9. `contextvars.ContextVar`（38 处）：异步世界里的"每个任务各自的全局变量"

推荐资料：Python 官方文档 asyncio "协程与任务" 一节；写几个 10 行的小例子自己跑一跑（这是练习，不是项目代码）。

### 项目对照阅读
- `frontier_agent/core/tool.py` 里 `Tool.ainvoke`：最简单的 `await`
- `frontier_agent/core/loop_types.py` 的 `notify_observers`、`drain_background_observers`
- `frontier_agent/core/runtime/loop/agent_loop.py` 的 `_wait_for_tool_interrupt`：`create_task` + 等待 + 取消的实战
- `frontier_agent/infra/session_context.py`：`ContextVar` 的实际用法
- `frontier_agent/infra/nonblocking_stream.py`（146 行）

**自测：**
- `await` 一个协程和 `create_task` 一个协程有什么区别？
- 如果用户在 TUI 里按了中断，正在执行的异步任务是怎么被停下来的？（先找 `CancelledError` 相关代码）

---

## Day 6：核心中的核心——Agent Loop（ReAct 循环）

### 背景概念（半小时）
- **ReAct**：LLM 思考 → 决定调用工具 → 执行工具 → 把结果喂回 LLM → 再思考……直到 LLM 不再调用工具，给出最终答案
- **OpenAI Chat Completions 接口**：`messages` 列表 + `tools` 列表 → 返回文本或 `tool_calls`。去 OpenAI 官方文档看一眼 function calling 的请求/响应示例

### 项目对照阅读（精读，今天最累）
按这个顺序：
1. `frontier_agent/core/runtime/loop/agent_loop.py`
   - 先读 `run_agent_loop`（入口，约 100 行）
   - 再读 `_run_loop_inner`：**这就是 ReAct 循环本体**，画出它的流程图
   - 然后按调用顺序读：`_prepare_llm_request` → `_call_llm_with_callbacks` → `_process_llm_response` → `_execute_tool_calls` → `_handle_turn_end` → `_finalize_loop`
2. `frontier_agent/core/runtime/loop/_tool.py`、`tool_exec.py`：工具是怎么被真正执行的
3. `frontier_agent/core/runtime/loop/tool_call_parser.py`：只读顶部说明和主函数——处理模型"把工具调用写成纯文本"的情况
4. `frontier_agent/core/loop_types.py` 回看 `BaseObserver` 的回调列表，对照 `docs/framework.md` 的 "Observer contract" 表格

**动手验证：**
```bash
uv run pytest tests/test_stateful_workflow.py -q
```
在 `_run_loop_inner` 的循环里放一个 `breakpoint()`，重跑测试，一步步看 `messages` 列表是怎么一轮轮变长的。

**自测（必须能画出来）：**
- 画一张图：一次 `run_agent_loop` 从开始到结束，经过了哪些函数，每个 observer 回调在哪个时机被触发
- `Intervention` 是干什么的？observer 怎么通过它"插手"循环？
- 循环在什么情况下结束？（至少说出 3 种）

---

## Day 7：LLM 客户端、Observer 生态、上下文压缩

### Python 知识点
- `httpx`（异步 HTTP 客户端）、`openai` / `anthropic` SDK 的基本用法
- `logging` 模块：`logging.getLogger(__name__)`（122 处）
- `contextlib`：`@contextmanager`、`suppress`（66 处）
- `functools`：`partial`、`lru_cache`、`wraps`

### 项目对照阅读
- `frontier_agent/core/llm.py`（93 行）：LLM 客户端的抽象接口
- `frontier_agent/infra/openai_client.py`：一个具体实现
- `frontier_agent/infra/retriable.py`、`infra/llm/fallback.py`：重试与降级
- `frontier_agent/core/runtime/loop/_call.py`、`_streaming.py`：调用 LLM 与流式接收（通读）
- `frontier_agent/components/observers/`：挑 4 个读——`last_turn_forcer.py`（最短）、`budget_observer.py`、`repetition_guard.py`、`trajectory.py`（轨迹记录）
- `frontier_agent/core/runtime/loop/compact.py`、`context_budget.py`：对话太长时怎么压缩（通读，理解"为什么要压缩、压缩了什么"即可）

**自测：**
- 从 `run_agent_loop` 到真正发出 HTTP 请求，中间经过了哪几层？
- `repetition_guard` 是怎么发现模型"卡住复读"并干预的？
- 为什么要做上下文压缩？`keep_last_k: 5` 在 `profiles/simple.yaml` 里是什么意思？

---

## Day 8：工作流（Workflow）——把 Loop 组装成产品

### Python 知识点
- `importlib.import_module`：按字符串动态导入模块（17 处）
- YAML 配置：`yaml.safe_load`，以及 `${OPENAI_MODEL}` 这种环境变量替换
- `os.environ`、`python-dotenv` 读 `.env`
- `pathlib.Path`（180 处）：路径操作

### 项目对照阅读
**A. 调度层（精读）**
- `frontier_agent/models/pipeline_spec.py`（回顾）
- `frontier_agent/scheduling/workflow_loader.py`、`pipeline_registry.py`、`scheduler.py`
- `frontier_agent/core/runtime/dag/minidag.py`、`graph_builder.py`：节点图是怎么执行的
- `frontier_agent/infra/config.py`：配置加载

**B. ReAct 工作流（精读）**
- `workflows/stateful_react_agent/README.md`
- `spec.py`（注意 `node_function` 是一个**字符串路径**，由 importlib 动态加载）
- `profiles/simple.yaml`、`benchmark.yaml`、`tui.yaml`：对比三个配置的差异
- `nodes/main_agent.py` 的 `react_agent_node`：看它怎么准备 prompt、tools、observers，然后调用 `run_agent_loop`
- `prompts.py`：系统提示词（system prompt）

**C. 读一遍官方开发文档**：`docs/workflows.md`

**自测：**
- 从 `REACT_SPEC` 这个对象出发，说明 scheduler 是如何最终调用到 `react_agent_node` 的
- `ContextPolicy.include_fields` 和 `output_fields` 分别控制什么？

---

## Day 9：Agent Team 多智能体 + 沙箱与安全

### 项目对照阅读
**A. Agent Team（通读，抓主干）**
- `workflows/agent_team/README.md`（精读）+ `assets/agent_team.png`
- `workflows/agent_team/spec.py`：和 ReAct 的 spec 对比
- `frontier_agent/components/agent_bus/`：先读 `__init__.py` 和 `models.py`，再读 `bus.py` 里的类和公开方法（1884 行，不必逐行）
- `spawn_guard.py`：限制子 agent 数量与嵌套深度
- 团队工具：`plugins/tools/create_subagent.py`、`assign_task.py`、`collect_reports.py`、`task_board.py`、`submit_report.py`
- `workflows/agent_team/nodes/main_agent.py`、`subagent_runtime.py`：只读顶层函数和注释

**B. 沙箱与安全（浏览，理解设计）**
- `docs/framework.md` 的 Sandboxing 一节（回顾）
- `plugins/tools/bash.py`：bash 工具入口
- `plugins/tools/_path_auth.py`：路径授权——为什么 `/inputs` 只读、`/outputs` 可写
- `plugins/tools/_bash_policy.py`、`_sandbox.py`：只看模块 docstring 和函数名列表（`grep -n "^def \|^class " 文件名`）
- `subprocess` 模块基础：`subprocess.run`、`asyncio.create_subprocess_exec`

**动手验证：**
```bash
uv run pytest tests/test_agent_team_workflow.py tests/test_path_authorization.py -q
```

**自测：**
- 主 agent 是怎么把任务派给子 agent、又怎么收回报告的？画出消息流向
- "fail-closed"（出错即拒绝）在路径授权里具体体现在哪？

---

## Day 10：终端产品 apodex（CLI + TUI）+ 全局串联

### Python 知识点
- `argparse`：命令行参数解析
- Textual / Rich：终端 UI 框架（只需理解"App → Screen → Widget"的层次和消息事件机制）
- `threading`（27 处）与 asyncio 的混用

### 项目对照阅读
- `apodex/README.md`、`docs/tui-user-guide.zh-CN.md`
- `apodex/cli.py`：`build_parser` → `main` → `_amain`，从命令行一路跟下去
- `apodex/session.py` 的 `TerminalSession` 与 `apodex/task_runner.py`：终端会话如何调用 `run_agent_loop`
- `apodex/observers.py`：observer 如何把 agent 的事件"桥接"到终端界面
- `apodex/permissions.py`、`changes.py`：操作审批与 `/revert` 回滚
- `apodex/tui/app.py`、`screens.py`、`widgets.py`：浏览类名和主要方法
- `benchmarks/README.md`、`docs/eval.md`：理解评测是怎么复用同一套引擎的

### 下午：全局串联（最重要的收尾）
不看代码，在白纸上画出并口述：
> 用户在终端输入 `frontier-agent`，输入一句"帮我调研 X 并写一份报告"，
> 到屏幕上出现最终报告，**中间经过的每一个关键文件和函数**。

然后打开代码逐一核对，补上遗漏的环节。能讲清楚这条链路，就说明你已经读懂了这个项目的主干。

---

## 附录 A：Python 知识点 × 项目出现频次速查

| 知识点 | 项目中出现次数（约） | 首次学习 |
|---|---|---|
| `async def` / `await` | 560 / 970 | Day 5 |
| `monkeypatch`（测试） | 430 | 读测试时 |
| `re.compile` 正则 | 240 | Day 4 |
| `lambda` | 210 | Day 2 |
| `Path(...)` | 180 | Day 8 |
| `async with` | 170 | Day 5 |
| `json.` | 140 | Day 4 |
| `logging.getLogger` | 120 | Day 7 |
| `@dataclass` | 80 | Day 3 |
| `Callable[...]` | 70 | Day 3 |
| `@property` | 65 | Day 2 |
| `Protocol` | 58 | Day 3 |
| `ContextVar` | 38 | Day 5 |
| `BaseModel`（pydantic） | 35 | Day 3 |
| `match / case` | 28 | 遇到时查 |

## 附录 B：读不懂时的套路

1. **先看测试**：`grep -rl "函数名" tests apodex/tests`，测试会告诉你"输入什么、期望输出什么"
2. **打断点**：`breakpoint()` + 跑相关测试，看真实数据
3. **查引用**：VS Code `Shift+F12`，看谁在调用它
4. **看 git 历史**：`git log -p --follow 文件名`，看它为什么被这样写
5. **只看签名**：`grep -n "^def \|^class \|    def " 文件名` 快速得到一个大文件的目录
6. **问 AI**：把一段看不懂的代码贴给 Claude，问"这段代码在整个项目里起什么作用"

## 附录 C：每日检查清单

- [ ] Day 1 能说出 5 个顶层目录的职责，知道 `frontier-agent` 命令的入口
- [ ] Day 2 能读懂 `ToolRegistry` 的每一行
- [ ] Day 3 能写出一条带 `tool_calls` 的 `Message` 结构
- [ ] Day 4 能解释 `@tool` 装饰器如何把函数变成 JSON Schema
- [ ] Day 5 能解释 `await` 与 `create_task` 的区别
- [ ] Day 6 能画出 `run_agent_loop` 的完整流程图
- [ ] Day 7 能说出 LLM 调用经过的各层
- [ ] Day 8 能说明 `PipelineSpec` 到节点函数执行的全过程
- [ ] Day 9 能画出 Agent Team 的任务派发与报告回收流程
- [ ] Day 10 能不看代码口述"从输入到输出"的完整链路

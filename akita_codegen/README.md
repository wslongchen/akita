# Akita CodeGen - CRUD Code Generator

## Overview

Akita CodeGen is a powerful Rust-based code generation tool specifically designed for the Akita framework to generate CRUD (Create, Read, Update, Delete) related code. By connecting to databases and analyzing table structures, it automatically generates entity classes, service layers, data access layers, controllers, and more, significantly improving development efficiency.

## Features

- 🚀 **Rapid Generation**: Generate complete CRUD code with one click
- 🔧 **Highly Configurable**: Rich configuration options to meet different project requirements
- 🎯 **Smart Naming**: Supports multiple naming strategies (camelCase, snake_case, etc.)
- 📁 **Template-Driven**: Template-based system supporting custom code templates
- 🛡️ **Type-Safe**: Built with Rust ensuring generated code quality
- 📊 **Multi-Table Support**: Batch generation for multiple tables
- 🔌 **Plugin System**: Extensible plugin functionality

## Installation

### From Source

```bash
# Clone repository
git clone <repository-url>

# Build project
cargo build --release

# Install locally
cargo install --path .
```

### Using Cargo

```bash
cargo install akita_codegen
```

## Quick Start

### 1. Basic Usage

```bash
# Generate code with default configuration
akita_codegen

# Specify configuration file
akita_codegen -c config.yaml
```

### 2. Interactive Configuration

If run for the first time without a configuration file, the tool enters interactive configuration mode:

```bash
$ akita_codegen
Would you like to set up the configuration interactively?
[Y/n]: Y

Enter your table_names (like 'table_a,table_b')
: user,product,order

Enter your database URL
: mysql://username:password@localhost:3306/mydatabase

Enter your template directory path
: ./templates

Enter your output directory path
: ./src/generated
```

### 3. Configuration File Example

Create a `config.yaml` file:

```yaml
global:
  output_dir: ./src/generated
  template_dir: ./templates/*
  file_override: true
  author: YourName
  entity_name: "%sEntity"
  service_name: "%sService"
  service_impl_name: "%sServiceImpl"
  mapper_name: "%sMapper"
  controller_name: "%sController"

package:
  parent: com.example.project
  module_name: demo
  entity: domain.entity
  service: domain.service
  service_impl: domain.service.impl
  mapper: infrastructure.mapper
  controller: application.controller

strategy:
  capital_mode: false
  naming: UnderlineToCamel
  column_naming: NoChange
  table_prefix: ["tbl_", "sys_"]
  include:
    - user
    - product
    - order
  exclude:
    - audit_log

datasource:
  driver_name: "com.mysql.cj.jdbc.Driver"
  username: root
  password: your_password
  url: mysql://username:password@localhost:3306/your_database?useSSL=false&serverTimezone=UTC
  db_type: Mysql

template:
  entity: ./templates/entity.rs
  service: ./templates/service.rs
  service_impl: ./templates/service_impl.rs
  controller: ./templates/controller.rs
  mapper: ./templates/mapper.rs

plugins: []
```

## Configuration Details

### Global Configuration (GlobalConfig)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `output_dir` | String | `""` | Code output directory |
| `template_dir` | String | `./templates` | Template file directory |
| `file_override` | bool | `true` | Whether to override existing files |
| `active_record` | bool | `false` | Whether to enable ActiveRecord mode |
| `open` | bool | `false` | Whether to automatically open directory after generation |
| `author` | String | `""` | Author information |
| `service_name` | String | `""` | Service class naming template (%s as placeholder) |
| `entity_name` | String | `""` | Entity class naming template |
| `service_impl_name` | String | `""` | Service implementation class naming template |
| `mapper_name` | String | `""` | Mapper class naming template |
| `controller_name` | String | `""` | Controller class naming template |

### Package Configuration (PackageConfig)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `parent` | String | `"com.snack"` | Parent package name |
| `module_name` | String | `""` | Module name |
| `entity` | String | `"entity"` | Entity class package name |
| `service` | String | `"service"` | Service interface package name |
| `service_impl` | String | `"service.impl"` | Service implementation package name |
| `mapper` | String | `"mapper"` | Data access layer package name |
| `controller` | String | `"controller"` | Controller package name |

### Strategy Configuration (StrategyConfig)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `capital_mode` | bool | `false` | Global uppercase naming mode |
| `naming` | NamingStrategy | `UnderlineToCamel` | Table naming strategy |
| `column_naming` | Option<NamingStrategy> | `NoChange` | Column naming strategy |
| `entity_boolean_column_remove_is_prefix` | bool | `false` | Whether to remove 'is' prefix from boolean fields |
| `table_prefix` | Vec<String> | `[]` | Table prefixes to remove |
| `field_prefix` | Vec<String> | `[]` | Field prefixes to remove |
| `include` | Vec<String> | `[]` | List of tables to include |
| `exclude` | Vec<String> | `[]` | List of tables to exclude |
| `skip_view` | bool | `false` | Whether to skip view tables |

### Data Source Configuration (DataSourceConfig)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `driver_name` | String | `""` | Database driver class name |
| `username` | String | `""` | Database username |
| `password` | String | `""` | Database password |
| `url` | String | `""` | Database connection URL |
| `db_type` | DbType | `Mysql` | Database type (currently supports Mysql) |

### Naming Strategies (NamingStrategy)

- `UnderlineToCamel`: Snake case to camelCase (e.g., `user_name` → `userName`)
- `CamelToUnderline`: CamelCase to snake_case (e.g., `userName` → `user_name`)
- `NoChange`: Keep original format

## Template System

### Default Template Structure

```
templates/
├── entity.rs          # Entity class template
├── service.rs         # Service interface template
├── serviceImpl.rs     # Service implementation template
├── controller.rs      # Controller template
└── mapper.rs          # Data access layer template
```

### Template Variables

The following variables are available in template files:

- `{{package}}`: Package name
- `{{imports}}`: Import statements
- `{{className}}`: Class name
- `{{tableName}}`: Table name
- `{{fields}}`: Field list
- `{{author}}`: Author
- `{{date}}`: Generation date

### Custom Templates

Create custom template files:

```rust
// templates/custom_entity.rs

/**
 * {{tableName}} Entity
 * 
 * @author {{author}}
 * @date {{date}}
 */
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct {{ entity }} {
    {%- for field in table.fields %}
    {%- if field.comment != "" %}
    /// {{ field.comment }}
    {% else %}
    {% endif -%}
    pub {{field.propertyName}}: Option<{{field.columnType}}>,
    {%- endfor %}
}
```

## Advanced Usage

### Batch Processing Multiple Tables

```yaml
strategy:
  include:
    - user
    - product
    - order_item
    - category
  exclude:
    - system_config
```

### Table Prefix Handling

```yaml
strategy:
  table_prefix:
    - "tbl_"
    - "sys_"
  include:
    - tbl_user
    - sys_log
```

### Field Fill Strategy

```yaml
strategy:
  table_fill_list:
    - field_name: create_time
      field_fill: Insert
    - field_name: update_time
      field_fill: InsertUpdate
```

### Custom Inheritance Classes

```yaml
strategy:
  super_entity_class: "com.example.base.BaseEntity"
  super_service_class: "com.example.base.BaseService"
  super_service_impl_class: "com.example.base.BaseServiceImpl"
  super_mapper_class: "com.example.base.BaseMapper"
  super_controller_class: "com.example.base.BaseController"
```

## Command Line Arguments

```bash
# Show help information
akita_codegen --help

# Show version information
akita_codegen --version

# Specify configuration file
akita_codegen --config custom-config.yaml

# Use short arguments
akita_codegen -c config.yaml
```

## Troubleshooting

### 1. Database Connection Failure

**Problem**: Unable to connect to database

**Solution**:
- Check database URL format: `mysql://username:password@host:port/database`
- Confirm database service is running
- Check network connection and firewall settings
- Verify username and password are correct

### 2. Template Files Not Found

**Problem**: Template files not found

**Solution**:
- Confirm `template_dir` configuration is correct
- Check if template files exist in the specified directory
- Use absolute or relative paths correctly

### 3. Generated Code Format Incorrect

**Problem**: Generated code format doesn't match expectations

**Solution**:
- Check naming strategy configuration
- Verify variables in template files
- Adjust package configuration and class naming templates

### 4. Cannot Read Table Structure

**Problem**: Unable to read database table structure

**Solution**:
- Ensure database user has sufficient permissions
- Check table names (case-sensitive)
- Verify database connection configuration

## Development Guide

### Extending the Plugin System

Create plugins by implementing the `Plugin` trait:

```rust
pub trait Plugin {
    fn name(&self) -> &str;
    fn execute(&self, context: &mut CodeGenContext) -> Result<(), Box<dyn Error>>;
}
```

### Custom Generation Strategies

Implement `CodeGenerationStrategy` trait for custom code generation logic:

```rust
pub trait CodeGenerationStrategy {
    fn generate_entity(&self, table: &TableInfo) -> Result<String>;
    fn generate_service(&self, table: &TableInfo) -> Result<String>;
    fn generate_mapper(&self, table: &TableInfo) -> Result<String>;
    fn generate_controller(&self, table: &TableInfo) -> Result<String>;
}
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the [MIT License](LICENSE).

## Support & Feedback

- Submit issues: [GitHub Issues](https://github.com/wslongchen/akita/issues)
- Discussion: [Discord/Forum Link]

---

**Note**: Code generated by this tool is for reference only. Please adjust and optimize according to your project requirements.
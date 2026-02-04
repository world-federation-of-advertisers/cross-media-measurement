# Architecture Documentation Agent

Analyze the directory or module at `$ARGUMENTS` and generate comprehensive architecture documentation.

## Instructions

1. **Explore the target directory** to understand its structure:
   - Identify all source files, their organization, and naming conventions
   - Map out the package/module hierarchy
   - Note any configuration files (BUILD.bazel, proto files, etc.)

2. **Analyze the code architecture**:
   - Identify key classes, interfaces, and their responsibilities
   - Map dependencies between components
   - Identify design patterns in use (e.g., Factory, Strategy, Repository)
   - Note any integration points with external systems or other modules

3. **Document the data flow**:
   - Trace how data enters and exits the module
   - Identify key data structures and their transformations
   - Note any async/concurrent patterns

4. **Generate documentation** that includes:
   - **Overview**: High-level purpose of the module/directory
   - **Component Diagram**: Text-based diagram of key components and their relationships
   - **Key Classes/Files**: Description of the most important files and their roles
   - **Design Patterns**: Patterns used and why
   - **Dependencies**: External and internal dependencies
   - **Data Flow**: How data moves through the module
   - **Extension Points**: Where and how the module can be extended

5. **Output format**: Write the documentation to `docs/architecture/<module-name>.md`

## Example Usage

```
/document-architecture src/main/kotlin/org/wfanet/measurement/edpaggregator
```

# n8n-nodes-flowengine-data-standardize-clean

**Stop writing Code Nodes to clean your data. Use this.**

A production-ready n8n Community Node that cleans and transforms messy data without writing a single line of code. Built with zero runtime dependencies for maximum compatibility and verification compliance.

![n8n Community Node](https://img.shields.io/badge/n8n-Community%20Node-orange)
![License](https://img.shields.io/badge/license-MIT-green)
![Zero Dependencies](https://img.shields.io/badge/runtime%20dependencies-0-brightgreen)

## The Problem

You're building automations in n8n, and your data is a mess:

- Names like `jOhN dOE` instead of `John Doe`
- Phone numbers in 15 different formats: `(555) 000-1111`, `555.000.1111`, `+1 555 000 1111`
- Duplicate contacts that are *almost* the same but not quite
- Emails with random whitespace and uppercase letters
- JSON with inconsistent key naming (`firstName`, `first_name`, `FirstName`)

**The old way:** Write complex JavaScript in Code Nodes, debug for hours, maintain spaghetti code.

**The new way:** Drop in the Data Cleaner node, configure in 30 seconds, done.

## Installation

### Via n8n Community Nodes

1. Go to **Settings** > **Community Nodes**
2. Select **Install**
3. Enter `n8n-nodes-flowengine-data-standardize-clean`
4. Click **Install**

### Manual Installation

```bash
# In your n8n installation directory
pnpm install n8n-nodes-flowengine-data-standardize-clean
```

## Operations

### 1. Deduplicate (Fuzzy)

Remove duplicate records using intelligent fuzzy matching. Perfect for cleaning contact lists, product catalogs, or any dataset with near-duplicates.

**How it works:**
- Uses Jaro-Winkler algorithm for short strings (names, emails)
- Uses Levenshtein distance for longer text
- Configurable similarity threshold (0.0 - 1.0)
- Compares across multiple fields simultaneously

**Example:**
```
Input:
  { "name": "John Smith", "email": "john@email.com" }
  { "name": "Jon Smith", "email": "john@email.com" }   // Typo in name
  { "name": "Jane Doe", "email": "jane@email.com" }

Output (threshold: 0.8):
  { "name": "John Smith", "email": "john@email.com" }
  { "name": "Jane Doe", "email": "jane@email.com" }
```

**Parameters:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| Fields to Check | Comma-separated field names to compare | Required |
| Fuzzy Threshold | Similarity threshold (0.0 - 1.0) | 0.8 |
| Output Duplicate Info | Include metadata about removed duplicates | false |

---

### 2. Clean Phone Numbers

Format phone numbers to E.164 international standard (`+15550001111`). Works with any input format.

**Handles:**
- `(555) 000-1111` → `+15550001111`
- `555.000.1111` → `+15550001111`
- `+44 20 7946 0958` → `+442079460958`
- `07946 0958` (UK) → `+447946095800`

**Parameters:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| Phone Field | Field containing phone number | `phone` |
| Default Country Code | Country code when not detected | `1` (US/Canada) |
| Output Field | Save to different field (optional) | Same as input |

---

### 3. Smart Capitalization

Convert text to proper Title Case with intelligent handling of common patterns.

**Examples:**
- `jOhN dOE` → `John Doe`
- `ACME CORPORATION` → `Acme Corporation`
- `mcdonald's` → `Mcdonald's`

**Handles exceptions:**
- Preserves acronyms: `IBM`, `NASA`, `CEO`
- Lowercase articles: `The Lord of the Rings`
- Roman numerals: `Henry VIII`

**Parameters:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| Fields to Capitalize | Comma-separated field names | Required |

---

### 4. Normalize Email

Clean and standardize email addresses.

**What it does:**
- Trims whitespace
- Converts to lowercase
- Corrects common domain typos (`gmial.com` → `gmail.com`)

**Example:**
- `  John.Doe@GMAIL.COM  ` → `john.doe@gmail.com`
- `jane@gmial.com` → `jane@gmail.com`

**Parameters:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| Email Field | Field containing email | `email` |
| Output Field | Save to different field (optional) | Same as input |

---

### 5. Clean Object Keys

Transform all JSON keys to consistent naming convention.

**Modes:**
- `snake_case`: `firstName` → `first_name`
- `camelCase`: `first_name` → `firstName`

**Features:**
- Recursively processes nested objects
- Handles arrays of objects
- Preserves values

**Example (snake_case):**
```json
// Before
{ "firstName": "John", "contactInfo": { "phoneNumber": "555-1234" } }

// After
{ "first_name": "John", "contact_info": { "phone_number": "555-1234" } }
```

**Parameters:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| Key Format | Target case format | `snake_case` |

## Why Zero Dependencies?

This node is built with **zero runtime dependencies** by design:

1. **Verification Ready**: Meets n8n Community Node verification requirements
2. **Security**: Smaller attack surface, no supply chain vulnerabilities
3. **Performance**: No bloat from unused library features
4. **Compatibility**: Works across all n8n versions
5. **Reliability**: No breaking changes from upstream dependencies

All algorithms (Jaro-Winkler, Levenshtein, phone parsing, etc.) are implemented natively in TypeScript with comprehensive documentation.

## Nested Field Support

All operations support dot notation for nested fields:

```
contact.personal.firstName
address.phone.mobile
user.emails[0]
```

## Development

```bash
# Install dependencies
pnpm install

# Build the node
pnpm build

# Watch for changes
pnpm dev

# Lint and format
pnpm lint
pnpm format
```

## Testing with n8n

```bash
# Link for local development
pnpm link --global

# In your n8n installation
pnpm link --global n8n-nodes-flowengine-data-standardize-clean

# Start n8n
n8n start
```

## License

[MIT](LICENSE.md) - Use it however you want.

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit PRs to the GitHub repository.

## Support

- **Issues**: [GitHub Issues](https://github.com/Ami3466/n8n-nodes-flowengine-data-standardize-clean/issues)
- **Documentation**: [n8n Community Nodes Docs](https://docs.n8n.io/integrations/community-nodes/)

---

**Built with care for the n8n community by FlowEngine.**

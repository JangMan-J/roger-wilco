import { homedir } from 'node:os';
import { join } from 'node:path';
import { readHostConfig } from '../bin/config.ts';
import { isNamespace, type HostConfig } from '../protocol/index.ts';

/** The host's provisioned config plus the namespace and Claude Code session locating fields. */
export type ClaudeConfig = HostConfig & {
  namespace: string;
  /** Explicit Claude Code session id; defaults to the harness-provided CLAUDE_CODE_SESSION_ID. */
  sessionId?: string;
  /** Explicit transcript path; defaults to discovery under the Claude Code projects root. */
  transcriptPath?: string;
  /** Root holding `<encoded-cwd>/<sessionId>.jsonl`; defaults to `$CLAUDE_CONFIG_DIR/projects` or `~/.claude/projects`. */
  projectsRoot: string;
};

export function defaultProjectsRoot(): string {
  return join(process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'), 'projects');
}

/** Explicit config or the provisioned user config; never a per-session destination file. */
export async function loadClaudeConfig(path?: string): Promise<ClaudeConfig> {
  const config = await readHostConfig(path);
  if (!isNamespace(config.namespace)) throw new Error('Invalid Claude Code namespace');
  return { ...config, namespace: config.namespace, projectsRoot: defaultProjectsRoot() };
}

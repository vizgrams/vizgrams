// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * EditPanel — collapsible Builder + YAML sections shown above explore frame results.
 *
 * Both sections are collapsed by default so results are the primary focus.
 * The Builder section is a placeholder until the visual builder is implemented.
 */
import { useState } from 'react'
import { ChevronDown, ChevronRight, Code2, Wrench } from 'lucide-react'
import { YamlEditor } from '@/components/YamlEditor'
import type { YamlEditorProps } from '@/components/YamlEditor'

interface EditPanelProps {
  yamlEditorProps: Omit<YamlEditorProps, 'headerSlot'>
  hideSaveButton?: boolean
}

export function CollapsibleSection({
  label,
  icon,
  open,
  onToggle,
  children,
  noPadding,
}: {
  label: string
  icon: React.ReactNode
  open: boolean
  onToggle: () => void
  children: React.ReactNode
  noPadding?: boolean
}) {
  return (
    <div className="border rounded-lg overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center gap-2 px-4 py-2.5 bg-muted/30 hover:bg-muted/50 transition-colors text-left"
      >
        {open
          ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
          : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
        }
        {icon}
        <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">{label}</span>
      </button>
      {open && <div className={noPadding ? '' : 'p-4'}>{children}</div>}
    </div>
  )
}

export function EditPanel({ yamlEditorProps, hideSaveButton }: EditPanelProps) {
  const [builderOpen, setBuilderOpen] = useState(false)
  const [yamlOpen, setYamlOpen] = useState(false)

  return (
    <div className="space-y-2">
      <CollapsibleSection
        label="Builder"
        icon={<Wrench className="h-3.5 w-3.5 text-muted-foreground" />}
        open={builderOpen}
        onToggle={() => setBuilderOpen(o => !o)}
      >
        <p className="text-sm text-muted-foreground">Visual builder coming soon.</p>
      </CollapsibleSection>

      <CollapsibleSection
        label="YAML"
        icon={<Code2 className="h-3.5 w-3.5 text-muted-foreground" />}
        open={yamlOpen}
        onToggle={() => setYamlOpen(o => !o)}
        noPadding
      >
        <YamlEditor {...yamlEditorProps} hideSaveButton={hideSaveButton} />
      </CollapsibleSection>
    </div>
  )
}

import { useEffect, useRef, useState, type ReactNode } from 'react'

/** Shared page shell: header + optional toolbar + body + optional sticky footer */
export function PageShell({
  title,
  subtitle,
  extra,
  toolbar,
  footer,
  bodyScroll = false,
  children,
}: {
  title: ReactNode
  subtitle?: ReactNode
  extra?: ReactNode
  toolbar?: ReactNode
  footer?: ReactNode
  /** allow body itself to scroll (forms); tables should keep false and use scroll.y */
  bodyScroll?: boolean
  children: ReactNode
}) {
  return (
    <div className="page-card">
      <div className="page-card-header">
        <div style={{ minWidth: 0 }}>
          <div style={{ fontSize: 16, fontWeight: 600, lineHeight: 1.4 }}>{title}</div>
          {subtitle ? (
            <div style={{ marginTop: 4, color: 'rgba(0,0,0,0.45)', fontSize: 13 }}>{subtitle}</div>
          ) : null}
        </div>
        {extra ? <div style={{ flexShrink: 0 }}>{extra}</div> : null}
      </div>
      {toolbar ? <div className="page-card-toolbar">{toolbar}</div> : null}
      <div className={`page-card-body${bodyScroll ? ' is-scroll' : ''}`}>{children}</div>
      {footer ? <div className="page-card-footer">{footer}</div> : null}
    </div>
  )
}

/** Measure available height for Ant Design Table scroll.y */
export function useTableScrollY(offset = 0) {
  const ref = useRef<HTMLDivElement>(null)
  const [y, setY] = useState(360)

  useEffect(() => {
    const el = ref.current
    if (!el) return

    const update = () => {
      // Subtract approximate table header height
      const header = el.querySelector('.ant-table-thead') as HTMLElement | null
      const headerH = header?.offsetHeight ?? 39
      const h = el.clientHeight - headerH - offset
      setY(Math.max(h, 160))
    }

    update()
    const ro = new ResizeObserver(() => requestAnimationFrame(update))
    ro.observe(el)
    window.addEventListener('resize', update)
    return () => {
      ro.disconnect()
      window.removeEventListener('resize', update)
    }
  }, [offset])

  return { containerRef: ref, scrollY: y }
}

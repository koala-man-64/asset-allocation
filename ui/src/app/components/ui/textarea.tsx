import * as React from 'react';

import { cn } from './utils';

function Textarea({ className, ...props }: React.ComponentProps<'textarea'>) {
  return (
    <textarea
      data-slot="textarea"
      className={cn(
        'resize-none border-2 border-mcm-walnut placeholder:text-mcm-olive/70 focus-visible:border-mcm-teal focus-visible:ring-mcm-teal/40 aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40 aria-invalid:border-destructive flex field-sizing-content min-h-20 w-full rounded-xl bg-input-background px-3 py-2 text-sm font-semibold transition-[color,box-shadow] outline-none focus-visible:ring-[3px] disabled:cursor-not-allowed disabled:opacity-50',
        className
      )}
      {...props}
    />
  );
}

export { Textarea };

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "./utils";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-full border-2 border-transparent text-[11px] font-black uppercase tracking-[0.18em] transition-[transform,box-shadow,color,background-color] duration-200 ease-out disabled:pointer-events-none disabled:opacity-50 hover:scale-[1.05] active:scale-[0.95] motion-reduce:transition-none motion-reduce:hover:scale-100 motion-reduce:active:scale-100 [&_svg]:pointer-events-none [&_svg:not([class*='size-'])]:size-4 shrink-0 [&_svg]:shrink-0 outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background",
  {
    variants: {
      variant: {
        default:
          "bg-mcm-mustard text-mcm-walnut border-mcm-walnut shadow-[3px_3px_0px_0px_rgba(119,63,26,0.2)] hover:bg-mcm-mustard/90",
        destructive:
          "bg-destructive text-destructive-foreground border-mcm-walnut/70 hover:bg-destructive/90",
        outline:
          "border-mcm-walnut bg-transparent text-mcm-walnut hover:bg-mcm-paper",
        secondary:
          "bg-mcm-cream text-mcm-walnut border-mcm-walnut shadow-[3px_3px_0px_0px_rgba(119,63,26,0.15)] hover:bg-mcm-paper",
        ghost: "text-mcm-walnut hover:bg-mcm-paper",
        link: "text-mcm-teal underline-offset-4 hover:underline",
      },
      size: {
        default: "h-10 px-5 py-2",
        sm: "h-9 px-4 text-[10px]",
        lg: "h-12 px-7 text-[12px]",
        icon: "size-10 px-0",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

const Button = React.forwardRef<
  HTMLButtonElement,
  React.ComponentProps<"button"> &
    VariantProps<typeof buttonVariants> & {
      asChild?: boolean;
    }
>(({ className, variant, size, asChild = false, ...props }, ref) => {
  const Comp = asChild ? Slot : "button";

  return (
    <Comp
      data-slot="button"
      className={cn(buttonVariants({ variant, size, className }))}
      ref={ref}
      {...props}
    />
  );
});

Button.displayName = "Button";

export { Button, buttonVariants };

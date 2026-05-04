// Tiny `cn` helper combining clsx + tailwind-merge so component
// authors can pass conditional class lists without worrying about
// Tailwind specificity collisions.
import clsx, { type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}

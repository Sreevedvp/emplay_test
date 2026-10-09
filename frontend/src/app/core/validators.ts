import { AbstractControl, AsyncValidatorFn, ValidationErrors, ValidatorFn } from '@angular/forms';
import { catchError, map, of, switchMap, timer, Observable } from 'rxjs';
import { Condition } from './models';
export const meaningfulName: ValidatorFn = (control: AbstractControl): ValidationErrors | null =>
  typeof control.value === 'string' &&
  control.value.trim().length >= 3 &&
  control.value.trim().length <= 80
    ? null
    : { meaningfulName: true };
/** Metric determines the allowed threshold range (cross-field validator). */
export const metricThreshold: ValidatorFn = (control: AbstractControl): ValidationErrors | null => {
  const c = control.value as Partial<Condition>;
  return typeof c.threshold === 'number' &&
    Number.isFinite(c.threshold) &&
    c.threshold >= 0 &&
    (c.metric !== 'errRate' || c.threshold <= 100)
    ? null
    : { metricThreshold: true };
};
export function uniqueName(
  check: (name: string) => Observable<{ available: boolean }>,
): AsyncValidatorFn {
  return (control) =>
    timer(350).pipe(
      switchMap(() => check(String(control.value).trim())),
      map((result) => (result.available ? null : { nameTaken: true })),
      catchError(() => of({ nameCheckUnavailable: true })),
    );
}

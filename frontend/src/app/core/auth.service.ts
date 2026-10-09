import { Injectable, inject, signal } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { ApiClient } from './api';
import { User } from './models';
@Injectable({ providedIn: 'root' })
export class Auth {
  private router = inject(Router);
  readonly user = signal<User | null>(null);
  readonly api = new ApiClient();
  constructor() {
    this.api.onSession = (session) => {
      this.user.set(session?.user ?? null);
      if (!session && !this.router.url.startsWith('/login'))
        void this.router.navigate(['/login'], { queryParams: { returnUrl: this.router.url } });
    };
  }
}
export const authenticated: CanActivateFn = (_route, state) => {
  const auth = inject(Auth);
  return auth.user()
    ? true
    : inject(Router).createUrlTree(['/login'], { queryParams: { returnUrl: state.url } });
};
export const operatorOnly: CanActivateFn = () =>
  inject(Auth).user()?.role === 'operator' ? true : inject(Router).createUrlTree(['/fleet']);

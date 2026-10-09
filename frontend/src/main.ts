import { bootstrapApplication } from '@angular/platform-browser';
import { provideRouter } from '@angular/router';
import { App } from './app/app';
import { authenticated, operatorOnly } from './app/core/auth.service';
bootstrapApplication(App, {
  providers: [
    provideRouter([
      { path: 'login', loadComponent: () => import('./app/features/login').then((m) => m.Login) },
      {
        path: 'fleet',
        canActivate: [authenticated],
        loadComponent: () => import('./app/features/fleet').then((m) => m.Fleet),
      },
      {
        path: 'services/:id',
        canActivate: [authenticated],
        loadComponent: () => import('./app/features/detail').then((m) => m.Detail),
      },
      {
        path: 'rules',
        canActivate: [authenticated, operatorOnly],
        canDeactivate: [(component: import('./app/features/rules').Rules) => component.canLeave()],
        loadComponent: () => import('./app/features/rules').then((m) => m.Rules),
      },
      {
        path: 'settings',
        canActivate: [authenticated],
        loadComponent: () => import('./app/features/settings').then((m) => m.Settings),
      },
      { path: '**', redirectTo: 'fleet' },
    ]),
  ],
}).catch(console.error);

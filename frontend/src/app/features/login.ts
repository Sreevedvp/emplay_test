import { ChangeDetectionStrategy, Component, inject, signal } from '@angular/core';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { MatButtonModule } from '@angular/material/button';
import { Auth } from '../core/auth.service';
@Component({
  selector: 'app-login',
  standalone: true,
  imports: [ReactiveFormsModule, MatButtonModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: ` <h1>Fleet Pulse</h1>
    <p>Sign in to the operations console.</p>
    <form [formGroup]="form" (ngSubmit)="login()" class="stack narrow">
      <label>Email <input type="email" autocomplete="username" formControlName="email" /></label>
      @if (form.controls.email.touched && form.controls.email.invalid) {
        <small role="alert">Enter a valid email.</small>
      }
      <label
        >Password <input type="password" autocomplete="current-password" formControlName="password"
      /></label>
      @if (form.controls.password.touched && form.controls.password.invalid) {
        <small role="alert">At least 8 characters required.</small>
      }
      <button mat-flat-button type="submit" [disabled]="form.invalid || pending()">
        {{ pending() ? 'Signing in…' : 'Sign in' }}
      </button>
      @if (error()) {
        <p role="alert">{{ error() }}</p>
      }
    </form>
    <p>Test accounts: operator&#64;pulse.dev or viewer&#64;pulse.dev<br />Password: pulse123</p>`,
})
export class Login {
  private auth = inject(Auth);
  private router = inject(Router);
  private route = inject(ActivatedRoute);
  form = inject(FormBuilder).nonNullable.group({
    email: ['operator@pulse.dev', [Validators.required, Validators.email]],
    password: ['', [Validators.required, Validators.minLength(8)]],
  });
  pending = signal(false);
  error = signal('');
  async login(): Promise<void> {
    if (this.form.invalid || this.pending()) return;
    this.pending.set(true);
    this.error.set('');
    try {
      const value = this.form.getRawValue();
      await this.auth.api.login(value.email, value.password);
      const target = this.route.snapshot.queryParamMap.get('returnUrl') ?? '/fleet';
      await this.router.navigateByUrl(
        target.startsWith('/') && !target.startsWith('//') && !target.startsWith('/login')
          ? target
          : '/fleet',
      );
    } catch (e) {
      this.error.set(String(e));
    } finally {
      this.pending.set(false);
    }
  }
}
